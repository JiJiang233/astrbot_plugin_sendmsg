import asyncio

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Nodes, Plain
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from astrbot_plugin_sendmsg.main import CollectedMessage, SendmsgPlugin, SendPlan


def test_send_plan_description() -> None:
    plan = SendPlan(groups=("100", "200"), friends=("300",))

    assert plan.total == 3
    assert plan.describe() == "2 个群、1 个好友"


def test_extract_ids_deduplicates_and_sorts() -> None:
    items = [
        {"group_id": 200},
        {"group_id": "10"},
        {"group_id": 200},
        {"ignored": 1},
    ]

    assert SendmsgPlugin._extract_ids(items, "group_id") == ["10", "200"]


def test_ensure_nonempty_rejects_empty_plan() -> None:
    try:
        SendmsgPlugin._ensure_nonempty(SendPlan())
    except ValueError as exc:
        assert "没有找到" in str(exc)
    else:
        raise AssertionError("empty plan should be rejected")


def test_one_collected_message_is_sent_directly() -> None:
    source = MessageChain([Plain("hello")])
    outgoing = SendmsgPlugin._build_outgoing_chain(
        [CollectedMessage(source, "10001", "Admin")]
    )

    assert len(outgoing.chain) == 1
    assert isinstance(outgoing.chain[0], Plain)


def test_multiple_collected_messages_become_forward_nodes() -> None:
    messages = [
        CollectedMessage(MessageChain([Plain("one")]), "10001", "Admin"),
        CollectedMessage(MessageChain([Plain("two")]), "10001", "Admin"),
    ]

    outgoing = SendmsgPlugin._build_outgoing_chain(messages)

    assert len(outgoing.chain) == 1
    assert isinstance(outgoing.chain[0], Nodes)
    assert len(outgoing.chain[0].nodes) == 2


def test_aiocqhttp_dispatches_direct_and_forward_messages() -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        async def send_group_msg(self, **kwargs) -> None:
            self.calls.append(("send_group_msg", kwargs))

        async def send_private_msg(self, **kwargs) -> None:
            self.calls.append(("send_private_msg", kwargs))

        async def call_action(self, action: str, **kwargs) -> None:
            self.calls.append((action, kwargs))

    async def run() -> FakeBot:
        bot = FakeBot()
        await AiocqhttpMessageEvent.send_message(
            bot=bot,
            message_chain=MessageChain([Plain("direct")]),
            is_group=True,
            session_id="123",
        )
        outgoing = SendmsgPlugin._build_outgoing_chain(
            [
                CollectedMessage(MessageChain([Plain("one")]), "1", "Admin"),
                CollectedMessage(MessageChain([Plain("two")]), "1", "Admin"),
            ]
        )
        await AiocqhttpMessageEvent.send_message(
            bot=bot,
            message_chain=outgoing,
            is_group=False,
            session_id="456",
        )
        return bot

    bot = asyncio.run(run())

    assert bot.calls[0][0] == "send_group_msg"
    assert bot.calls[1][0] == "send_private_forward_msg"
    assert len(bot.calls[1][1]["messages"]) == 2
