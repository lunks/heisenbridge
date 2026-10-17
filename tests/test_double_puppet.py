import asyncio
from types import SimpleNamespace

from mautrix.types import MessageEvent

from heisenbridge.plumbed_room import PlumbedRoom
from heisenbridge.private_room import PrivateRoom
from heisenbridge.room import DOUBLE_PUPPET_KEY
from heisenbridge.room import is_double_puppeted

OWNER = "@owner:example.com"


class Collector:
    def __init__(self):
        self.events = []

    def enqueue(self, event):
        self.events.append(event)


def _room(cls=PrivateRoom, double_puppet=True, lazy=True):
    serv = SimpleNamespace(
        irc_user_id=lambda network, nick: f"@irc_{network}_{nick.lower()}:example.com",
        is_user_cached=lambda user_id, nick: True,
        puppet_prefix="irc_",
        server_name="example.com",
    )

    async def make():
        # EventQueue needs a running loop, it's replaced by a collector below
        return cls("", OWNER, serv, [OWNER], [])

    room = asyncio.run(make())
    room.name = "#chan"
    room.network = SimpleNamespace(
        name="net",
        double_puppet=double_puppet,
        conn=SimpleNamespace(real_nickname="me"),
        pills_length=0,
        pills_ignore=[],
        color=True,
        caps=[],
        forward=False,
    )
    room.lazy_members = {} if lazy else None
    room._queue = Collector()
    return room


def _irc(source, *args):
    return SimpleNamespace(source=SimpleNamespace(nick=source), arguments=list(args), tags=[])


def _mx(content):
    return MessageEvent.deserialize(
        {
            "type": "m.room.message",
            "room_id": "!room:example.com",
            "event_id": "$event",
            "sender": OWNER,
            "origin_server_ts": 0,
            "content": content,
        }
    )


def test_own_privmsg_is_double_puppeted():
    room = _room()
    room.on_privmsg(None, _irc("me", "hello"))
    (event,) = room._queue.events
    assert event["user_id"] == OWNER
    assert event["content"]["body"] == "hello"
    assert event["content"][DOUBLE_PUPPET_KEY] is True


def test_own_action_and_notice_are_double_puppeted():
    room = _room()
    room.on_ctcp(None, _irc("me", "ACTION", "waves"))
    room.on_privnotice(None, _irc("me", "psst"))
    emote, notice = room._queue.events
    assert (emote["user_id"], emote["content"]["msgtype"], emote["content"]["body"]) == (OWNER, "m.emote", "waves")
    assert (notice["user_id"], notice["content"]["msgtype"], notice["content"]["body"]) == (OWNER, "m.notice", "psst")
    assert emote["content"][DOUBLE_PUPPET_KEY] and notice["content"][DOUBLE_PUPPET_KEY]


def test_other_nick_is_not_double_puppeted():
    room = _room()
    room.on_privmsg(None, _irc("someone", "hi"))
    (event,) = room._queue.events
    assert event["user_id"] == "@irc_net_someone:example.com"
    assert DOUBLE_PUPPET_KEY not in event["content"]


def test_disabled_keeps_puppet_behaviour():
    room = _room(double_puppet=False)
    room.on_privmsg(None, _irc("me", "hello"))
    (event,) = room._queue.events
    assert event["user_id"] == "@irc_net_me:example.com"
    assert DOUBLE_PUPPET_KEY not in event["content"]


def test_plumbed_room_never_double_puppets():
    room = _room(cls=PlumbedRoom)
    assert not room.can_double_puppet()


def test_marked_events_are_not_relayed_back():
    room = _room()
    seen = []

    async def handler(event):
        seen.append(event)

    room._mx_handlers["m.room.message"] = [handler]

    async def run():
        await room.on_mx_event(_mx({"msgtype": "m.text", "body": "from irc", DOUBLE_PUPPET_KEY: True}))
        await room.on_mx_event(_mx({"msgtype": "m.text", "body": "from matrix"}))

    asyncio.run(run())
    assert [str(e.content.body) for e in seen] == ["from matrix"]
    assert is_double_puppeted(_mx({"msgtype": "m.emote", "body": "x", DOUBLE_PUPPET_KEY: True}))
