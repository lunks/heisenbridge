import asyncio
import logging

import pytest
from mautrix.appservice import AppServiceAPI
from mautrix.appservice import IntentAPI
from mautrix.errors import IntentError
from mautrix.types import EventType
from mautrix.types import StateEvent

from heisenbridge.__main__ import MemoryBridgeStateStore

BOT = "@heisenbridge:example.com"
OTHER = "@other:example.com"
ROOM = "!room:example.com"


def _create(version, sender=BOT, additional_creators=None):
    content = {"room_version": version}
    if additional_creators is not None:
        content["additional_creators"] = additional_creators
    return StateEvent.deserialize(
        {
            "type": "m.room.create",
            "room_id": ROOM,
            "event_id": "$create",
            "sender": sender,
            "state_key": "",
            "origin_server_ts": 0,
            "content": content,
        }
    )


def _power_levels():
    # room v12: creators are not listed in users
    return StateEvent.deserialize(
        {
            "type": "m.room.power_levels",
            "room_id": ROOM,
            "event_id": "$pl",
            "sender": BOT,
            "state_key": "",
            "origin_server_ts": 0,
            "content": {"users": {}, "users_default": 0, "state_default": 50, "events": {}},
        }
    )


def _check_topic_power(mxid, create):
    # everything is cached, so no request ever reaches the (unreachable) homeserver
    async def run():
        store = MemoryBridgeStateStore()
        await store.update_state(create)
        await store.update_state(_power_levels())
        api = AppServiceAPI(
            "http://localhost:1", bot_mxid=BOT, token="x", log=logging.getLogger("test"), state_store=store
        )
        intent = IntentAPI(mxid, api, state_store=store)
        try:
            await intent._ensure_has_power_level_for(ROOM, EventType.ROOM_TOPIC)
        finally:
            await api.session.close()

    asyncio.run(run())


def test_v12_creator_can_set_topic():
    _check_topic_power(BOT, _create("12"))


def test_v12_additional_creator_can_set_topic():
    _check_topic_power(OTHER, _create("12", additional_creators=[OTHER]))


def test_v12_non_creator_cannot_set_topic():
    with pytest.raises(IntentError):
        _check_topic_power(OTHER, _create("12"))


def test_v11_creator_gets_no_implicit_power():
    with pytest.raises(IntentError):
        _check_topic_power(BOT, _create("11"))
