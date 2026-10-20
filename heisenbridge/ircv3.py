"""
Opt-in IRCv3 features: server-time + chathistory backfill, reply/react tags, echo-message and FILEHOST.

Everything here is inert unless a per-network toggle is on (see the network room commands BACKFILL,
REPLIES, ECHO and FILEHOST), so the default behaviour of the bridge is unchanged.
"""

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

LABEL_PREFIX = "hb"
REPLY_TAGS = ("+draft/reply", "+reply")
REACT_TAGS = ("+draft/react", "+react")
UNREACT_TAGS = ("+draft/unreact", "+unreact")

# events that carry a chat message
MESSAGE_EVENTS = ("privmsg", "pubmsg", "privnotice", "pubnotice", "ctcp")

SEEN_MAX = 500
MAP_MAX = 200


def parse_server_time(value: Optional[str]) -> Optional[int]:
    """ISO 8601 `time` tag to unix milliseconds."""
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return int(dt.timestamp() * 1000)


def format_server_time(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def escape_tag_value(value: str) -> str:
    out = []
    for c in value:
        out.append({";": "\\:", " ": "\\s", "\\": "\\\\", "\r": "\\r", "\n": "\\n"}.get(c, c))
    return "".join(out)


def build_tags(tags: Dict[str, Optional[str]]) -> str:
    """`{'label': 'x', '+draft/reply': 'id'}` -> `@label=x;+draft/reply=id` (empty for no tags)."""
    if not tags:
        return ""
    parts = []
    for k, v in tags.items():
        parts.append(k if v is None else f"{k}={escape_tag_value(str(v))}")
    return "@" + ";".join(parts)


@dataclass
class MsgMeta:
    """IRCv3 metadata of one IRC message."""

    msgid: Optional[str] = None
    ts: Optional[int] = None  # unix milliseconds
    reply: Optional[str] = None
    react: Optional[str] = None
    unreact: Optional[str] = None
    label: Optional[str] = None
    batch: Optional[str] = None
    account: Optional[str] = None
    backfill: bool = False

    def keep_separate(self) -> bool:
        # events with their own identity must never be merged with their neighbours
        return bool(self.msgid or self.reply or self.backfill)


def meta_from_event(event) -> MsgMeta:
    meta = MsgMeta()
    for tag in getattr(event, "tags", None) or []:
        key, value = tag["key"], tag["value"]
        if key == "msgid":
            meta.msgid = value
        elif key == "time":
            meta.ts = parse_server_time(value)
        elif key in REPLY_TAGS:
            meta.reply = meta.reply or value
        elif key in REACT_TAGS:
            meta.react = value
        elif key in UNREACT_TAGS:
            meta.unreact = value
        elif key == "label":
            meta.label = value
        elif key == "batch":
            meta.batch = value
        elif key == "account":
            meta.account = value
    return meta


def event_meta(event) -> Optional[MsgMeta]:
    return getattr(event, "meta", None)


class MsgIndex:
    """Bounded per-room memory of IRC msgids: dedupe plus msgid <-> Matrix event id."""

    def __init__(self, seen_max: int = SEEN_MAX, map_max: int = MAP_MAX):
        self.seen_max = seen_max
        self.map_max = map_max
        self.seen: "OrderedDict[str, None]" = OrderedDict()
        self.events: "OrderedDict[str, str]" = OrderedDict()
        self.by_event: Dict[str, str] = {}

    def is_seen(self, msgid: str) -> bool:
        return msgid in self.seen

    def add_seen(self, msgid: str) -> bool:
        """Remember msgid, False if it was already known."""
        if msgid in self.seen:
            return False
        self.seen[msgid] = None
        while len(self.seen) > self.seen_max:
            self.seen.popitem(last=False)
        return True

    def link(self, msgid: str, event_id: str) -> None:
        self.add_seen(msgid)
        self.events[msgid] = event_id
        self.by_event.setdefault(event_id, msgid)
        while len(self.events) > self.map_max:
            old_msgid, old_event = self.events.popitem(last=False)
            if self.by_event.get(old_event) == old_msgid:
                del self.by_event[old_event]

    def event_for(self, msgid: Optional[str]) -> Optional[str]:
        return self.events.get(msgid) if msgid else None

    def msgid_for(self, event_id: Optional[str]) -> Optional[str]:
        return self.by_event.get(event_id) if event_id else None

    def dump(self) -> List[List[str]]:
        return [[m, e] for m, e in self.events.items()]

    def load(self, pairs) -> None:
        for pair in pairs or []:
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                self.link(pair[0], pair[1])


def select_window(candidates: list, cap: int) -> Tuple[list, int]:
    """Keep the newest `cap` of the chronological candidates, return (kept, skipped count)."""
    if cap <= 0:
        return [], len(candidates)
    if len(candidates) <= cap:
        return list(candidates), 0
    return list(candidates[-cap:]), len(candidates) - cap


class Batch:
    def __init__(self, kind: str, params: List[str]):
        self.kind = kind
        self.params = params
        self.events: list = []


class IRCv3:
    """Per-network IRCv3 feature manager, owned by a NetworkRoom."""

    HISTORY_PAGE = 500
    HISTORY_PAGES = 4
    HISTORY_TIMEOUT = 30
    LABEL_KEEP = 256

    def __init__(self, network):
        self.network = network
        self.conn_id = 0
        self.batches: Dict[str, Batch] = {}
        self.history_lock = asyncio.Lock()
        self.history_waiter: Optional[asyncio.Future] = None
        self.pending_labels: "OrderedDict[str, Optional[str]]" = OrderedDict()
        self.label_counter = 0
        self.token_waiter: Optional[asyncio.Future] = None
        self.token_chunks: List[str] = []
        self.token_lock = asyncio.Lock()
        # lines we sent to our own nick: the server delivers them back as an ordinary message too
        self.self_sent: list = []

    # ---- capabilities -------------------------------------------------------------------------

    def wanted_caps(self) -> List[str]:
        n = self.network
        caps: List[str] = []
        if n.backfill:
            caps += ["server-time", "batch", "draft/chathistory"]
        if n.replies:
            caps += ["message-tags"]
        if n.echo:
            caps += ["message-tags", "echo-message", "labeled-response"]
        if n.filehost:
            caps += ["batch", "draft/authtoken"]
        return list(dict.fromkeys(caps))

    def enabled(self, cap: str) -> bool:
        return cap in (self.network.caps_enabled or [])

    @property
    def active(self) -> bool:
        n = self.network
        return bool(n.backfill or n.replies or n.echo)

    @property
    def backfill_active(self) -> bool:
        return bool(self.network.backfill and self.enabled("draft/chathistory") and self.enabled("batch"))

    @property
    def echo_active(self) -> bool:
        return bool(self.network.echo and self.enabled("echo-message") and self.enabled("labeled-response"))

    @property
    def replies_active(self) -> bool:
        return bool(self.network.replies and self.enabled("message-tags"))

    def tags_active(self) -> bool:
        return self.echo_active or self.replies_active

    def reset_connection(self) -> None:
        self.conn_id += 1
        self.batches = {}
        self.pending_labels = OrderedDict()
        self.self_sent = []
        for fut in (self.history_waiter, self.token_waiter):
            if fut is not None and not fut.done():
                fut.cancel()
        self.history_waiter = None
        self.token_waiter = None
        self.token_chunks = []
        self.history_lock = asyncio.Lock()
        self.token_lock = asyncio.Lock()

    def register(self, conn) -> None:
        for name in (*MESSAGE_EVENTS, "action", "tagmsg"):
            conn.add_global_handler(name, self.on_message, -50)
        conn.add_global_handler("batch", self.on_batch, -50)
        conn.add_global_handler("fail", self.on_fail, -50)
        conn.add_global_handler("token", self.on_token, -50)
        conn.add_global_handler("ack", self.on_ack, -50)

    # ---- helpers ------------------------------------------------------------------------------

    def room_for(self, conn, event):
        try:
            if event.target != conn.real_nickname:
                target = event.target.lower()
            else:
                target = event.source.nick.lower()
        except AttributeError:
            return None
        return self.network.rooms.get(target)

    # ---- batches ------------------------------------------------------------------------------

    def on_batch(self, conn, event) -> Optional[str]:
        ref = str(event.target or "")
        if not ref or ref[0] not in "+-":
            return None

        if ref[0] == "+":
            args = list(event.arguments)
            self.batches[ref[1:]] = Batch(args[0] if args else "", args[1:])
            return "NO MORE"

        batch = self.batches.pop(ref[1:], None)
        if batch is not None and batch.kind == "chathistory":
            if self.history_waiter is not None and not self.history_waiter.done():
                self.history_waiter.set_result(batch.events)
        elif batch is not None and batch.kind == "draft/authtoken" and batch.params and batch.params[0] != "*":
            if self.token_waiter is not None and not self.token_waiter.done():
                self.token_waiter.set_result(True)
        return "NO MORE"

    def on_fail(self, conn, event) -> Optional[str]:
        if str(event.target).upper() == "CHATHISTORY" and self.history_waiter is not None:
            if not self.history_waiter.done():
                self.history_waiter.set_result(None)
            return "NO MORE"
        if str(event.target).upper() == "TOKEN" and self.token_waiter is not None:
            if not self.token_waiter.done():
                self.token_waiter.set_result(None)
            return "NO MORE"
        return None

    def on_ack(self, conn, event) -> Optional[str]:
        # labeled-response acknowledgement of a command that had nothing else to say
        return "NO MORE" if self.pending_labels or self.active else None

    def on_token(self, conn, event) -> Optional[str]:
        if str(event.target).upper() == "GENERATE" and len(event.arguments) >= 2:
            self.token_chunks.append(event.arguments[1])
        return "NO MORE"

    # ---- the per message hook -----------------------------------------------------------------

    def on_message(self, conn, event) -> Optional[str]:
        if not self.active and not self.batches:
            return None

        if event.type == "action":
            # CTCP ACTION generates both ctcp and action, rooms only handle ctcp
            return "NO MORE" if self._in_history_batch(event) else None

        meta = meta_from_event(event)
        event.meta = meta

        # chat history replay is collected and posted by the backfill task
        batch = self.batches.get(meta.batch) if meta.batch else None
        if batch is not None:
            if batch.kind == "chathistory" and self._is_chat(event):
                batch.events.append(event)
            return "NO MORE"

        if not self.active:
            return None

        # echo of something we sent ourselves, it is already in Matrix
        if meta.label and meta.label.startswith(LABEL_PREFIX):
            self._on_echo(conn, event, meta)
            return "NO MORE"

        room = self.room_for(conn, event)
        if room is None:
            return None
        if not getattr(room, "allow_v3", False):
            event.meta = None
            return None

        if self._is_self_delivery(conn, event, room, meta):
            return "NO MORE"

        replay = getattr(event, "v3_replay", False)

        if not replay and room.backfilling and event.type != "tagmsg":
            # keep live messages behind the history being replayed to preserve order
            room.held.append(event)
            return "NO MORE"

        if meta.msgid and not room.index.add_seen(meta.msgid):
            return "NO MORE"

        if meta.msgid and event.type != "tagmsg":
            self.advance_cursor(room, meta)

        # live messages only get the server time when backfill (and with it server-time) is on
        if not (self.network.backfill and self.enabled("server-time")):
            meta.ts = None

        return None

    def note_self_sent(self, sig, event_id: Optional[str]) -> None:
        self.self_sent.append((sig, event_id, time.monotonic()))
        del self.self_sent[:-20]

    def _is_self_delivery(self, conn, event, room, meta: MsgMeta) -> bool:
        """The copy of a line we sent to our own nick that the server delivers to us besides the echo."""
        if not self.self_sent or event.source is None or event.source.nick != conn.real_nickname:
            return False
        if event.target != conn.real_nickname:
            return False
        if event.type == "tagmsg":
            sig = ("tag", meta.react or meta.unreact, meta.reply)
        elif event.type == "ctcp" and len(event.arguments) > 1:
            sig = event.arguments[1]
        elif event.type in ("privmsg", "privnotice"):
            sig = event.arguments[0]
        else:
            return False
        now = time.monotonic()
        self.self_sent[:] = [e for e in self.self_sent if now - e[2] < 30]
        for i, (sent, event_id, _) in enumerate(self.self_sent):
            if sent == sig:
                del self.self_sent[i]
                if meta.msgid:
                    room.index.add_seen(meta.msgid)
                    if event_id:
                        room.index.link(meta.msgid, event_id)
                    if event.type != "tagmsg":
                        self.advance_cursor(room, meta)
                return True
        return False

    @staticmethod
    def _is_chat(event) -> bool:
        if event.type == "ctcp":
            return bool(event.arguments) and event.arguments[0] == "ACTION"
        return event.type in MESSAGE_EVENTS

    def _in_history_batch(self, event) -> bool:
        for tag in getattr(event, "tags", None) or []:
            if tag["key"] == "batch":
                batch = self.batches.get(tag["value"])
                return batch is not None and batch.kind == "chathistory"
        return False

    def advance_cursor(self, room, meta: MsgMeta) -> None:
        room.irc_cursor = (meta.msgid, meta.ts or int(time.time() * 1000))
        room.mark_dirty()

    def _on_echo(self, conn, event, meta: MsgMeta) -> None:
        event_id = self.pending_labels.pop(meta.label, None)
        room = self.room_for(conn, event)
        if room is None or not getattr(room, "allow_v3", False) or not meta.msgid:
            return
        room.index.add_seen(meta.msgid)
        if event_id:
            room.index.link(meta.msgid, event_id)
        if event.type != "tagmsg":
            self.advance_cursor(room, meta)
        room.mark_dirty()

    # ---- outgoing -----------------------------------------------------------------------------

    def register_label(self, event_id: Optional[str]) -> str:
        self.label_counter += 1
        label = f"{LABEL_PREFIX}{self.conn_id}-{self.label_counter}"
        self.pending_labels[label] = event_id
        while len(self.pending_labels) > self.LABEL_KEEP:
            self.pending_labels.popitem(last=False)
        return label

    def outgoing_tags(self, room, event, first: bool) -> Dict[str, Optional[str]]:
        """Tags for one line the owner sends from Matrix."""
        tags: Dict[str, Optional[str]] = {}
        content = getattr(event, "content", None)
        is_edit = bool(content is not None and content.get_edit())
        if first and self.replies_active and content is not None and not is_edit:
            msgid = room.index.msgid_for(content.get_reply_to())
            if msgid:
                tags["+draft/reply"] = msgid
        if self.echo_active:
            event_id = getattr(event, "event_id", None) if first and not is_edit else None
            tags["label"] = self.register_label(event_id)
        return tags

    # ---- chathistory --------------------------------------------------------------------------

    async def chathistory(self, target: str, subcommand: str, ref: str, limit: int) -> Optional[list]:
        """Run one CHATHISTORY query, returns the replayed message events or None on error."""
        async with self.history_lock:
            loop = asyncio.get_running_loop()
            self.history_waiter = loop.create_future()
            try:
                self.network.conn.send_items("CHATHISTORY", subcommand, target, ref, str(limit))
                return await asyncio.wait_for(self.history_waiter, self.HISTORY_TIMEOUT)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                return None
            finally:
                self.history_waiter = None

    def start_backfill(self, room) -> None:
        """Called when the room's channel is joined (once per connection)."""
        if not self.backfill_active or not getattr(room, "allow_backfill", False):
            return
        if room.backfilling or room.backfill_conn_id == self.conn_id:
            return
        room.backfill_conn_id = self.conn_id
        # set synchronously so live messages are held from now on
        room.backfilling = True
        asyncio.ensure_future(self.backfill_room(room))

    async def backfill_room(self, room) -> None:
        try:
            await self._backfill(room)
        except Exception:
            logging.exception(f"Backfill of {room.name} failed")
        finally:
            room.backfilling = False
            held, room.held = room.held, []
            conn = self.network.conn
            for event in held:
                event.v3_replay = True
                try:
                    if conn is not None:
                        conn.reactor._handle_event(conn, event)
                except Exception:
                    logging.exception("Replaying held message failed")

    async def _backfill(self, room) -> None:
        n = self.network
        cursor = room.irc_cursor

        if cursor is None:
            # nothing to continue from, remember where the channel is now and post nothing
            events = await self.chathistory(room.name, "LATEST", "*", 1)
            if events:
                meta = meta_from_event(events[-1])
                if meta.msgid:
                    room.index.add_seen(meta.msgid)
                    self.advance_cursor(room, meta)
            return

        cursor_msgid, cursor_ts = cursor
        max_age_ms = max(1, int(n.backfill_max_age)) * 3600 * 1000
        now_ms = int(time.time() * 1000)
        stale = cursor_ts < now_ms - max_age_ms
        # timestamps survive history expiry, an unknown msgid just returns an empty batch
        start_ts = max(cursor_ts - 1, now_ms - max_age_ms)
        room.index.add_seen(cursor_msgid)

        collected: list = []
        ref = "timestamp=" + format_server_time(start_ts)
        truncated = False
        for page in range(self.HISTORY_PAGES):
            events = await self.chathistory(room.name, "AFTER", ref, self.HISTORY_PAGE)
            if events is None:
                if not collected:
                    return
                break
            collected.extend(events)
            if len(events) < self.HISTORY_PAGE:
                break
            last = meta_from_event(events[-1])
            if not last.ts:
                break
            ref = "timestamp=" + format_server_time(last.ts - 1)
            if page == self.HISTORY_PAGES - 1:
                truncated = True

        candidates = []
        newest = None
        seen_here = set()
        for event in collected:
            meta = meta_from_event(event)
            if not meta.msgid or meta.msgid in seen_here:
                continue
            seen_here.add(meta.msgid)
            newest = meta
            if room.index.is_seen(meta.msgid):
                continue
            # without echo-message there is no telling our own bridge-sent lines from another client's
            if event.source.nick == n.conn.real_nickname and not self.echo_active:
                continue
            candidates.append((event, meta))

        window, skipped = select_window(candidates, int(n.backfill_max))

        if skipped or stale:
            note = f"{skipped} older" if skipped else "Older"
            more = " or more" if truncated else ""
            room.send_notice(
                f"Backfill: {note}{more} messages from while the bridge was offline were skipped"
                f" (limit {n.backfill_max} messages / {n.backfill_max_age} h)."
            )

        for event, meta in window:
            meta.backfill = True
            event.meta = meta
            room.index.add_seen(meta.msgid)
            handler = getattr(room, "on_" + event.type, None)
            if handler is None:
                continue
            try:
                handler(n.conn, event)
            except Exception:
                logging.exception("Posting backfilled message failed")

        if newest is not None and newest.msgid:
            room.index.add_seen(newest.msgid)
            self.advance_cursor(room, newest)

    # ---- filehost -----------------------------------------------------------------------------

    @property
    def filehost_url(self) -> Optional[str]:
        conn = self.network.conn
        if conn is None:
            return None
        url = getattr(conn.features, "obby.world/filehost", None)
        return url if isinstance(url, str) and url.startswith(("http://", "https://")) else None

    @property
    def filehost_active(self) -> bool:
        return bool(
            self.network.filehost
            and self.filehost_url
            and self.enabled("draft/authtoken")
            and self.enabled("batch")
            and self.network.conn is not None
            and self.network.conn.connected
        )

    async def filehost_token(self, service: str = "FILEHOST") -> Optional[str]:
        async with self.token_lock:
            loop = asyncio.get_running_loop()
            self.token_waiter = loop.create_future()
            self.token_chunks = []
            try:
                self.network.conn.send_items("TOKEN", "GENERATE", service)
                await asyncio.wait_for(self.token_waiter, 10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.token_chunks = []
            finally:
                self.token_waiter = None
            token = "".join(self.token_chunks)
            self.token_chunks = []
            return token or None

    async def filehost_upload(self, data: bytes, filename: str, mimetype: Optional[str]) -> Optional[str]:
        """Upload to the network's file host, returns the public URL or None on any failure."""
        import aiohttp

        if not self.filehost_active:
            return None
        try:
            token = await self.filehost_token()
            if not token:
                return None
            base = self.filehost_url.rstrip("/")
            form = aiohttp.FormData()
            form.add_field("file", data, filename=filename, content_type=mimetype or "application/octet-stream")
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
                async with session.post(
                    base + "/upload", data=form, headers={"Authorization": f"Bearer {token}"}
                ) as resp:
                    if resp.status not in (200, 201):
                        logging.warning(f"Filehost upload failed with HTTP {resp.status}")
                        return None
                    saved = (await resp.json(content_type=None)).get("saved_url")
            if not saved:
                return None
            return saved if saved.startswith(("http://", "https://")) else base + "/" + saved.lstrip("/")
        except Exception:
            logging.exception("Filehost upload failed")
            return None
