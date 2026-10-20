import asyncio
import time
from types import SimpleNamespace

from mautrix.types import Event
from mautrix.types import MessageEvent

from heisenbridge.channel_room import ChannelRoom
from heisenbridge.event_queue import EventQueue
from heisenbridge.irc import HeisenReactor
from heisenbridge.ircv3 import build_tags
from heisenbridge.ircv3 import format_server_time
from heisenbridge.ircv3 import IRCv3
from heisenbridge.ircv3 import meta_from_event
from heisenbridge.ircv3 import MsgIndex
from heisenbridge.ircv3 import parse_server_time
from heisenbridge.ircv3 import select_window
from heisenbridge.plumbed_room import PlumbedRoom
from heisenbridge.private_room import PrivateRoom
from heisenbridge.room import DOUBLE_PUPPET_KEY

OWNER = "@owner:example.com"
ALICE = "@irc_net_alice:example.com"
T0 = int(time.time() * 1000) - 3_600_000


def iso(ms):
    return format_server_time(ms)


class Collector:
    def __init__(self):
        self.events = []

    def enqueue(self, event):
        self.events.append(event)

    def messages(self):
        return [e for e in self.events if e["type"] == "m.room.message"]


class Harness:
    """A real HeisenConnection fed with raw IRC lines, plus a ChannelRoom behind the IRCv3 hook."""

    def __init__(self, cls=ChannelRoom, name="#chan", **net):
        loop = asyncio.get_running_loop()
        self.reactor = HeisenReactor(loop=loop)
        self.conn = self.reactor.server()
        self.conn.real_nickname = "me"
        self.conn.real_server_name = "srv"
        self.conn.handlers = {}
        self.conn.connected = True
        self.sent = []
        self.server = None  # callable(line) -> list of raw lines to answer with

        def send_raw(string, priority=0, tag=None):
            self.sent.append(string)
            if self.server:
                for line in self.server(string) or []:
                    loop.call_soon(self.feed, line)

        self.conn.send_raw = send_raw

        serv = SimpleNamespace(
            irc_user_id=lambda network, nick: f"@irc_{network}_{nick.lower()}:example.com",
            is_user_cached=lambda user_id, nick: True,
            puppet_prefix="irc_",
            server_name="example.com",
        )
        self.network = SimpleNamespace(
            name="net",
            double_puppet=False,
            conn=self.conn,
            pills_length=0,
            pills_ignore=[],
            color=True,
            caps=[],
            forward=False,
            rooms={},
            backfill=False,
            backfill_max=200,
            backfill_max_age=24,
            replies=False,
            echo=False,
            filehost=False,
            caps_enabled=["message-tags", "server-time", "batch", "draft/chathistory"],
            real_user="u",
            real_host="h",
        )
        for k, v in net.items():
            setattr(self.network, k, v)
        self.network.v3 = IRCv3(self.network)
        self.v3 = self.network.v3

        room = cls("!room:example.com", OWNER, serv, [OWNER, ALICE], [])
        room.name = name
        room.network = self.network
        room.lazy_members = {}
        room._queue = Collector()
        self.saves = 0

        async def save():
            self.saves += 1

        room.save = save
        self.room = room
        self.network.rooms[name] = room

        self.leaks = []
        self.v3.register(self.conn)
        for kind in ("pubmsg", "privmsg", "pubnotice", "privnotice", "ctcp"):
            self.conn.add_global_handler(kind, self._dispatch)
        self.conn.add_global_handler("tagmsg", self._dispatch)
        self.conn.add_global_handler("unhandled_events", lambda c, e: self.leaks.append(e.type))

    def _dispatch(self, conn, event):
        room = self.v3.room_for(conn, event)
        if room is not None:
            getattr(room, "on_" + event.type)(conn, event)

    def feed(self, line):
        self.conn._process_line(line)

    @property
    def posted(self):
        return self.room._queue.messages()


def run(coro):
    return asyncio.run(coro)


# ---- helpers ------------------------------------------------------------------------------------


def test_server_time_roundtrip():
    assert parse_server_time("2026-10-19T15:11:47.869Z") == 1792422707869
    assert format_server_time(1792422707869) == "2026-10-19T15:11:47.869Z"
    assert parse_server_time("garbage") is None
    assert parse_server_time(None) is None


def test_build_tags_escapes():
    assert build_tags({}) == ""
    assert build_tags({"label": "a", "+draft/reply": "id"}) == "@label=a;+draft/reply=id"
    assert build_tags({"+draft/react": "a b;c\\"}) == "@+draft/react=a\\sb\\:c\\\\"


def test_meta_from_tags_reads_both_reply_spellings():
    ev = SimpleNamespace(
        tags=[
            {"key": "msgid", "value": "m1"},
            {"key": "time", "value": "2026-10-19T15:11:47.869Z"},
            {"key": "+reply", "value": "m0"},
            {"key": "+draft/react", "value": "x"},
            {"key": "account", "value": "bob"},
            {"key": "label", "value": "hb1"},
        ]
    )
    meta = meta_from_event(ev)
    assert (meta.msgid, meta.reply, meta.react, meta.account, meta.label) == ("m1", "m0", "x", "bob", "hb1")
    assert meta.ts == 1792422707869


def test_index_is_bounded_and_roundtrips():
    idx = MsgIndex(seen_max=5, map_max=3)
    for i in range(10):
        idx.link(f"m{i}", f"$e{i}")
    assert list(idx.events) == ["m7", "m8", "m9"]
    assert idx.msgid_for("$e9") == "m9" and idx.event_for("m9") == "$e9"
    assert idx.msgid_for("$e0") is None
    assert len(idx.seen) == 5
    other = MsgIndex()
    other.load(idx.dump())
    assert other.event_for("m8") == "$e8"
    assert not other.add_seen("m8")


def test_select_window_keeps_newest():
    assert select_window([1, 2, 3], 5) == ([1, 2, 3], 0)
    assert select_window([1, 2, 3, 4, 5], 2) == ([4, 5], 3)
    assert select_window([1, 2], 0) == ([], 2)


def test_events_with_meta_are_never_merged():
    async def go():
        flushed = []

        async def cb(events):
            flushed.append(events)

        q = EventQueue(cb)
        q.start()
        base = {"type": "m.room.message", "user_id": ALICE}
        q.enqueue({**base, "content": {"msgtype": "m.text", "body": "a"}})
        q.enqueue({**base, "content": {"msgtype": "m.text", "body": "b"}})
        q.enqueue({**base, "content": {"msgtype": "m.text", "body": "c"}, "meta": object()})
        q.enqueue({**base, "content": {"msgtype": "m.text", "body": "d"}, "meta": object()})
        await asyncio.sleep(0.3)
        q.stop()
        return [[e["content"]["body"] for e in batch] for batch in flushed]

    # a+b merge into one event, c and d stay separate
    assert run(go()) == [["a\nb"], ["c"], ["d"]]


def test_tagged_connection_send():
    async def go():
        h = Harness()
        h.conn.send_tagged({"label": "x", "+draft/reply": "m1"}, "PRIVMSG", "#chan", ":hi")
        h.conn.send_tagged({}, "PRIVMSG", "#chan", ":plain")
        return h.sent

    assert run(go()) == ["@label=x;+draft/reply=m1 PRIVMSG #chan :hi", "PRIVMSG #chan :plain"]


# ---- live messages ------------------------------------------------------------------------------


def privmsg(msgid, ts, text, nick="alice", extra="", target="#chan"):
    return f"@msgid={msgid};time={iso(ts)}{extra} :{nick}!u@h PRIVMSG {target} :{text}"


def test_inactive_hook_changes_nothing():
    async def go():
        h = Harness()
        h.feed(privmsg("m1", T0, "hi"))
        (ev,) = h.posted
        return ev, h.room.irc_cursor

    ev, cursor = run(go())
    assert "meta" not in ev and cursor is None


def test_live_message_gets_server_time_and_is_deduped():
    async def go():
        h = Harness(backfill=True)
        h.feed(privmsg("m1", T0, "hi"))
        h.feed(privmsg("m1", T0, "hi"))
        h.feed(privmsg("m2", T0 + 5, "again"))
        return h

    h = run(go())
    assert [e["content"]["body"] for e in h.posted] == ["hi", "again"]
    assert [e["meta"].ts for e in h.posted] == [T0, T0 + 5]
    assert h.room.irc_cursor == ("m2", T0 + 5)


def test_live_time_not_used_without_backfill():
    async def go():
        h = Harness(replies=True)
        h.feed(privmsg("m1", T0, "hi"))
        return h

    (ev,) = run(go()).posted
    assert ev["meta"].msgid == "m1" and ev["meta"].ts is None


def test_plumbed_rooms_are_left_alone():
    async def go():
        h = Harness(cls=PlumbedRoom, backfill=True)
        h.feed(privmsg("m1", T0, "hi"))
        return h

    h = run(go())
    assert all("meta" not in e for e in h.room._queue.events)
    assert h.room.irc_cursor is None


# ---- backfill -----------------------------------------------------------------------------------


def history_server(h, messages, calls=None):
    """Answer CHATHISTORY with a batch, like Ergo: timestamp= returns what is newer than it."""

    def server(line):
        if not line.startswith("CHATHISTORY"):
            return []
        calls is not None and calls.append(line)
        _, sub, target, ref, limit = line.split(" ")
        if sub == "LATEST":
            picked = messages[-int(limit) :]
        else:
            ts = parse_server_time(ref.split("=", 1)[1])
            picked = [m for m in messages if m[1] > ts][: int(limit)]
        out = [f":srv BATCH +7 chathistory {target}"]
        for msgid, mts, nick, text in picked:
            out.append(f"@batch=7;msgid={msgid};time={iso(mts)};account={nick} :{nick}!u@h PRIVMSG {target} :{text}")
        out.append(":srv BATCH -7")
        return out

    return server


def test_backfill_posts_missed_messages_with_original_time():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m1", T0)
        h.room.index.add_seen("m1")
        msgs = [("m1", T0, "alice", "old"), ("m2", T0 + 1000, "alice", "missed one"), ("m3", T0 + 2000, "bob", "two")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    h = run(go())
    assert [e["content"]["body"] for e in h.posted] == ["missed one", "two"]
    assert [e["meta"].ts for e in h.posted] == [T0 + 1000, T0 + 2000]
    assert [e["user_id"] for e in h.posted] == [ALICE, "@irc_net_bob:example.com"]
    assert h.room.irc_cursor == ("m3", T0 + 2000)
    assert h.leaks == []
    # asked with the cursor time, one millisecond early, never with the unreliable msgid
    assert h.sent[0].startswith(f"CHATHISTORY AFTER #chan timestamp={iso(T0 - 1)}")


def test_backfill_cap_posts_newest_and_one_notice():
    async def go():
        h = Harness(backfill=True, backfill_max=3)
        h.room.irc_cursor = ("m0", T0)
        msgs = [(f"m{i}", T0 + i * 1000, "alice", f"line {i}") for i in range(1, 11)]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    h = run(go())
    bodies = [e["content"]["body"] for e in h.posted]
    assert bodies[0].startswith("Backfill: 7 older messages")
    assert bodies[1:] == ["line 8", "line 9", "line 10"]
    assert h.room.irc_cursor[0] == "m10"


def test_backfill_never_reposts_known_messages():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m1", T0)
        h.room.index.link("m2", "$posted")  # already in Matrix, cursor just did not advance
        msgs = [("m1", T0, "alice", "a"), ("m2", T0 + 1, "alice", "b"), ("m3", T0 + 2, "alice", "c")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        await h.v3._backfill(h.room)  # and a second pass is a no-op
        return h

    h = run(go())
    assert [e["content"]["body"] for e in h.posted] == ["c"]


def test_backfill_skips_own_nick_without_echo():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m0", T0)
        msgs = [("m1", T0 + 1, "me", "sent from matrix"), ("m2", T0 + 2, "alice", "hi")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    assert [e["content"]["body"] for e in run(go()).posted] == ["hi"]


def test_backfill_own_nick_from_other_client_is_double_puppeted_with_echo():
    async def go():
        h = Harness(
            backfill=True,
            echo=True,
            double_puppet=True,
            caps_enabled=["server-time", "batch", "draft/chathistory", "echo-message", "labeled-response"],
        )
        h.room.irc_cursor = ("m0", T0)
        msgs = [("m1", T0 + 1, "me", "from web chat")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    (ev,) = run(go()).posted
    assert ev["user_id"] == OWNER and ev["content"][DOUBLE_PUPPET_KEY] is True


def test_backfill_without_cursor_only_records_the_channel_position():
    async def go():
        h = Harness(backfill=True)
        msgs = [("m1", T0, "alice", "a"), ("m2", T0 + 1, "alice", "b")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    h = run(go())
    assert h.posted == []
    assert h.room.irc_cursor == ("m2", T0 + 1)


def test_backfill_old_cursor_is_limited_by_max_age():
    async def go():
        h = Harness(backfill=True, backfill_max_age=2)
        now = int(time.time() * 1000)
        h.room.irc_cursor = ("m0", now - 10 * 3600 * 1000)
        msgs = [("m1", now - 5 * 3600 * 1000, "alice", "too old"), ("m2", now - 3600 * 1000, "alice", "recent")]
        h.server = history_server(h, msgs)
        await h.v3._backfill(h.room)
        return h

    bodies = [e["content"]["body"] for e in run(go()).posted]
    assert bodies[0].startswith("Backfill: Older messages")
    assert bodies[1:] == ["recent"]


def test_backfill_error_posts_nothing_and_keeps_cursor():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m0", T0)
        h.server = lambda line: [":srv FAIL CHATHISTORY INVALID_PARAMS :nope"] if line.startswith("CHAT") else []
        await h.v3._backfill(h.room)
        return h

    h = run(go())
    assert h.posted == [] and h.room.irc_cursor == ("m0", T0)


def test_live_messages_wait_for_backfill_and_are_deduped_against_it():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m0", T0)
        msgs = [("m1", T0 + 1, "alice", "history 1"), ("m2", T0 + 2, "alice", "history 2")]
        h.server = history_server(h, msgs)
        h.v3.start_backfill(h.room)
        # live traffic during the replay: one new, one that history is about to deliver too
        h.feed(privmsg("m3", T0 + 3, "live new"))
        h.feed(privmsg("m2", T0 + 2, "history 2"))
        assert h.posted == []
        await asyncio.sleep(0.1)
        return h

    h = run(go())
    assert [e["content"]["body"] for e in h.posted] == ["history 1", "history 2", "live new"]
    assert h.room.irc_cursor[0] == "m3"
    assert not h.room.backfilling


def test_backfill_runs_once_per_connection():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m0", T0)
        calls = []
        h.server = history_server(h, [], calls)
        h.v3.start_backfill(h.room)
        await asyncio.sleep(0.05)
        h.v3.start_backfill(h.room)
        await asyncio.sleep(0.05)
        first = len(calls)
        h.v3.reset_connection()
        h.v3.start_backfill(h.room)
        await asyncio.sleep(0.05)
        return first, len(calls)

    assert run(go()) == (1, 2)


def test_backfill_needs_negotiated_caps():
    async def go():
        h = Harness(backfill=True, caps_enabled=["message-tags"])
        h.room.irc_cursor = ("m0", T0)
        h.v3.start_backfill(h.room)
        return h

    h = run(go())
    assert not h.room.backfilling and h.sent == []


# ---- echo-message -------------------------------------------------------------------------------


ECHO_CAPS = ["message-tags", "echo-message", "labeled-response"]


def matrix_event(content, event_id="$mx1", sender=OWNER):
    return MessageEvent.deserialize(
        {
            "type": "m.room.message",
            "room_id": "!room:example.com",
            "event_id": event_id,
            "sender": sender,
            "origin_server_ts": 0,
            "content": content,
        }
    )


def test_matrix_send_is_labeled_and_its_echo_is_not_posted_back():
    async def go():
        h = Harness(echo=True, replies=True, caps_enabled=ECHO_CAPS)
        ev = matrix_event({"msgtype": "m.text", "body": "hello"})
        h.room._irc_emit(h.conn.privmsg, "hello", ev, True)
        label = h.sent[0].split(" ")[0]
        assert h.sent[0] == f"{label} PRIVMSG #chan :hello" and label.startswith("@label=hb")
        # server echo, then a second copy as if we messaged ourselves
        echo = f"{label[1:]};msgid=mine;time={iso(T0)}"
        h.feed(f"@{echo} :me!u@h PRIVMSG #chan :hello")
        h.feed(f"@msgid=mine;time={iso(T0)} :me!u@h PRIVMSG #chan :hello")
        return h

    h = run(go())
    assert h.posted == []
    assert h.room.index.event_for("mine") == "$mx1"
    assert h.room.irc_cursor[0] == "mine"
    assert h.leaks == []


def test_action_from_matrix_is_labeled_ctcp():
    async def go():
        h = Harness(echo=True, caps_enabled=ECHO_CAPS)
        h.room._irc_emit(h.conn.action, "waves", matrix_event({"msgtype": "m.emote", "body": "waves"}), True)
        return h.sent

    (line,) = run(go())
    assert line.endswith("PRIVMSG #chan :\x01ACTION waves\x01") and line.startswith("@label=hb")


def test_without_echo_matrix_sends_are_untouched():
    async def go():
        h = Harness(caps_enabled=ECHO_CAPS)
        h.room._irc_emit(h.conn.privmsg, "hello", matrix_event({"msgtype": "m.text", "body": "hello"}), True)
        return h.sent

    assert run(go()) == ["PRIVMSG #chan :hello"]


def test_other_session_message_is_still_double_puppeted_with_echo_on():
    async def go():
        h = Harness(echo=True, double_puppet=True, caps_enabled=ECHO_CAPS)
        h.feed(privmsg("web1", T0, "from the web client", nick="me"))
        return h

    (ev,) = run(go()).posted
    assert ev["user_id"] == OWNER and ev["content"][DOUBLE_PUPPET_KEY] is True


# ---- replies and reactions ----------------------------------------------------------------------


def test_irc_reply_becomes_matrix_reply_and_msgid_is_linked_on_send():
    class Intent:
        def __init__(self):
            self.sent = []

        def user(self, uid):
            return self

        async def send_message_event(self, room_id, type, content, **kwargs):
            self.sent.append((content, kwargs))
            return f"$ev{len(self.sent)}"

    async def go():
        h = Harness(replies=True, backfill=True)
        h.room.az = SimpleNamespace(intent=Intent())
        h.feed(privmsg("m1", T0, "question"))
        h.feed(privmsg("m2", T0 + 10, "answer", nick="alice", extra=";+draft/reply=m1"))
        await h.room._flush_events(h.room._queue.events)
        return h

    h = run(go())
    first, second = h.room.az.intent.sent
    assert first[1] == {"timestamp": T0} and "m.relates_to" not in first[0]
    assert second[0]["m.relates_to"] == {"m.in_reply_to": {"event_id": "$ev1"}}
    assert h.room.index.event_for("m2") == "$ev2" and h.room.index.msgid_for("$ev1") == "m1"


def test_irc_reaction_becomes_matrix_reaction():
    async def go():
        h = Harness(replies=True)
        h.room.index.link("m1", "$ev1")
        h.feed("@msgid=r1;+draft/react=x;+draft/reply=m1 :alice!u@h TAGMSG #chan")
        h.feed("@msgid=r2;+draft/react=y;+draft/reply=unknown :alice!u@h TAGMSG #chan")
        h.feed("@msgid=r3;+typing=active :alice!u@h TAGMSG #chan")
        return h

    h = run(go())
    (ev,) = [e for e in h.room._queue.events if e["type"] == "m.reaction"]
    assert ev["content"]["m.relates_to"] == {"rel_type": "m.annotation", "event_id": "$ev1", "key": "x"}
    assert ev["user_id"] == ALICE and ev["drop_absent"]


def test_own_reaction_from_other_client_is_double_puppeted_only_when_enabled():
    async def go(dp):
        h = Harness(replies=True, double_puppet=dp)
        h.room.index.link("m1", "$ev1")
        h.feed("@msgid=r1;+draft/react=x;+draft/reply=m1 :me!u@h TAGMSG #chan")
        return [e for e in h.room._queue.events if e["type"] == "m.reaction"]

    assert run(go(False)) == []
    (ev,) = run(go(True))
    assert ev["user_id"] == OWNER and ev["content"][DOUBLE_PUPPET_KEY] is True


def test_reactions_off_by_default():
    async def go():
        h = Harness()
        h.room.index.link("m1", "$ev1")
        h.feed("@msgid=r1;+draft/react=x;+draft/reply=m1 :alice!u@h TAGMSG #chan")
        return h

    assert run(go()).room._queue.events == []


def test_matrix_reply_gets_reply_tag_only_when_target_known():
    async def go():
        h = Harness(replies=True)
        h.room.index.link("mid1", "$known")
        known = matrix_event(
            {"msgtype": "m.text", "body": "re", "m.relates_to": {"m.in_reply_to": {"event_id": "$known"}}}
        )
        unknown = matrix_event(
            {"msgtype": "m.text", "body": "re", "m.relates_to": {"m.in_reply_to": {"event_id": "$other"}}}
        )
        h.room._irc_emit(h.conn.privmsg, "first line", known, True)
        h.room._irc_emit(h.conn.privmsg, "second line", known, False)
        h.room._irc_emit(h.conn.privmsg, "x", unknown, True)
        return h.sent

    assert run(go()) == [
        "@+draft/reply=mid1 PRIVMSG #chan :first line",
        "PRIVMSG #chan :second line",
        "PRIVMSG #chan :x",
    ]


def test_matrix_reaction_becomes_tagmsg_only_when_msgid_known():
    async def go():
        h = Harness(replies=True, echo=True, caps_enabled=ECHO_CAPS)
        h.conn.connected = True
        h.room.index.link("mid1", "$known")

        def reaction(target, event_id="$r1", sender=OWNER):
            return Event.deserialize(
                {
                    "type": "m.reaction",
                    "room_id": "!room:example.com",
                    "event_id": event_id,
                    "sender": sender,
                    "origin_server_ts": 0,
                    "content": {"m.relates_to": {"rel_type": "m.annotation", "event_id": target, "key": "\U0001f44d"}},
                }
            )

        await h.room.on_mx_reaction(reaction("$known"))
        await h.room.on_mx_reaction(reaction("$nope", "$r2"))
        await h.room.on_mx_reaction(reaction("$known", "$r3", "@someone:example.com"))
        sent = list(h.sent)
        # redacting the reaction withdraws it
        await h.room.on_mx_redaction(SimpleNamespace(redacts="$r1"))
        return sent, h.sent[len(sent) :]

    sent, withdrawn = run(go())
    assert len(sent) == 1 and sent[0].endswith("TAGMSG #chan")
    assert "+draft/react=\U0001f44d" in sent[0] and "+draft/reply=mid1" in sent[0] and "label=hb" in sent[0]
    assert len(withdrawn) == 1 and "+draft/unreact=\U0001f44d" in withdrawn[0]


def test_own_tagmsg_echo_is_dropped():
    async def go():
        h = Harness(replies=True, echo=True, caps_enabled=ECHO_CAPS, double_puppet=True)
        h.room.index.link("m1", "$ev1")
        label = h.v3.register_label(None)
        h.feed(f"@label={label};msgid=r9;+draft/react=x;+draft/reply=m1 :me!u@h TAGMSG #chan")
        return h

    assert run(go()).room._queue.events == []


# ---- persistence --------------------------------------------------------------------------------


def test_cursor_and_mapping_survive_a_restart():
    async def go():
        h = Harness(backfill=True)
        h.room.irc_cursor = ("m9", T0)
        h.room.index.link("m8", "$e8")
        cfg = h.room.to_config()
        h2 = Harness(backfill=True)
        h2.room.from_config({**cfg, "name": "#chan", "network_id": "x"})
        return h2.room

    room = run(go())
    assert room.irc_cursor == ("m9", T0)
    assert room.index.event_for("m8") == "$e8" and room.index.is_seen("m9")


def test_unused_features_add_nothing_to_room_config():
    async def go():
        return Harness().room.to_config()

    assert "irc_v3" not in run(go())


def test_private_room_class_flags():
    assert PrivateRoom.allow_v3 and not PrivateRoom.allow_backfill
    assert ChannelRoom.allow_backfill
    assert not PlumbedRoom.allow_v3 and not PlumbedRoom.allow_backfill


# ---- file host ----------------------------------------------------------------------------------


def test_filehost_token_is_collected_from_the_authtoken_batch():
    async def go():
        h = Harness(filehost=True)
        h.network.caps_enabled = ["batch", "draft/authtoken"]

        def server(line):
            if line == "TOKEN GENERATE FILEHOST":
                return [
                    ":srv BATCH +9 draft/authtoken FILEHOST",
                    "@batch=9 TOKEN GENERATE * aaaa",
                    "@batch=9 TOKEN GENERATE * bbbb",
                    ":srv BATCH -9",
                ]

        h.server = server
        return await h.v3.filehost_token(), h

    token, h = run(go())
    assert token == "aaaabbbb" and h.leaks == []


def test_filehost_token_failure_returns_none():
    async def go():
        h = Harness(filehost=True)
        h.server = lambda line: [":srv FAIL TOKEN UNKNOWN_SERVICE FILEHOST :Unknown service"]
        return await h.v3.filehost_token()

    assert run(go()) is None


def test_filehost_inactive_without_server_support():
    async def go():
        h = Harness(filehost=True)
        h.network.caps_enabled = ["batch", "draft/authtoken"]
        no_feature = h.v3.filehost_active
        h.conn.features.set("obby.world/FILEHOST", "https://host.example/media")
        return no_feature, h.v3.filehost_active, h.v3.filehost_url

    assert run(go()) == (False, True, "https://host.example/media")
