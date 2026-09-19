from __future__ import annotations

import asyncio
import json
import random
import re
from dataclasses import dataclass
from typing import Any, Literal

from aiocqhttp.exceptions import ActionFailed
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Node, Nodes, Plain
from astrbot.api.star import Context, Star
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.star.filter.permission import PermissionType
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from astrbot.core.utils.session_waiter import (
    SessionController,
    SessionFilter,
    session_waiter,
)

PLUGIN_NAME = "astrbot_plugin_sendmsg"
WAIT_TIMEOUT_SECONDS = 120
MIN_SEND_INTERVAL_SECONDS = 0.5
MAX_SEND_INTERVAL_SECONDS = 3.0

HELP_TEXT = """AstrBot 群发消息

用法：
/sendmsg <群号|QQ号>  向单个群或好友发送
/sendmsg group all    向所有群发送
/sendmsg friend all   向所有好友发送
/sendmsg all          向所有群和好友发送
/sendmsg help         显示本帮助

执行命令后，可在当前会话连续发送多条内容；发送“发送”开始群发，发送“取消”终止。
只收集 1 条时直接发送；收集 2 条及以上时合并为消息记录。
若数字 ID 同时存在于群列表和好友列表，可使用 /sendmsg group <群号> 或 /sendmsg friend <QQ号> 明确指定。"""

TargetKind = Literal["group", "friend"]


@dataclass(frozen=True)
class SendPlan:
    groups: tuple[str, ...] = ()
    friends: tuple[str, ...] = ()

    @property
    def total(self) -> int:
        return len(self.groups) + len(self.friends)

    def describe(self) -> str:
        parts: list[str] = []
        if self.groups:
            parts.append(f"{len(self.groups)} 个群")
        if self.friends:
            parts.append(f"{len(self.friends)} 个好友")
        return "、".join(parts) or "0 个目标"


@dataclass(frozen=True)
class CollectedMessage:
    chain: MessageChain
    sender_id: str
    sender_name: str


class ExactSessionFilter(SessionFilter):
    """只接收命令发起者在同一 Bot、同一会话发送的后续消息。"""

    def filter(self, event: AstrMessageEvent) -> str:
        return (
            f"{event.get_platform_name()}|{event.get_platform_id()}|"
            f"{event.get_self_id()}|{event.unified_msg_origin}|"
            f"{event.get_sender_id()}"
        )


class SendmsgPlugin(Star):
    def __init__(self, context: Context) -> None:
        super().__init__(context)

    @filter.permission_type(PermissionType.ADMIN)
    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.command("sendmsg")
    async def sendmsg(
        self,
        event: AiocqhttpMessageEvent,
        target: str = "",
        value: str = "",
    ):
        """向指定 QQ 群、好友或全部联系人发送下一条消息。"""
        target = target.strip().lower()
        value = value.strip().lower()

        if not target or target in {"help", "帮助"}:
            yield event.plain_result(HELP_TEXT)
            return

        try:
            plan = await self._build_plan(event, target, value)
        except ValueError as exc:
            yield event.plain_result(f"参数错误：{exc}\n\n{HELP_TEXT}")
            return
        except Exception as exc:  # noqa: BLE001 - OneBot 客户端异常类型不固定
            logger.exception("[%s] 获取发送目标失败", PLUGIN_NAME)
            yield event.plain_result(f"获取群/好友列表失败：{exc}")
            return

        yield event.plain_result(
            f"已选择 {plan.describe()}。请连续发送要群发的内容，每次等待上限为 "
            f"{WAIT_TIMEOUT_SECONDS} 秒。发送“发送”开始群发，发送“取消”终止。"
        )
        collected: list[CollectedMessage] = []

        @session_waiter(timeout=WAIT_TIMEOUT_SECONDS, record_history_chains=False)
        async def wait_for_content(
            controller: SessionController,
            content_event: AstrMessageEvent,
        ) -> None:
            content_event.stop_event()
            instruction = self._get_instruction(content_event)
            if instruction in {"取消", "cancel"}:
                controller.stop()
                await content_event.send(content_event.plain_result("已取消群发。"))
                return

            if instruction in {"发送", "send"}:
                if not collected:
                    await content_event.send(
                        content_event.plain_result(
                            "尚未收集任何消息，请先发送内容，或发送“取消”。"
                        )
                    )
                    controller.keep(timeout=WAIT_TIMEOUT_SECONDS, reset_timeout=True)
                    return

                controller.stop()
                await content_event.send(
                    content_event.plain_result(
                        f"已收集 {len(collected)} 条消息，开始向 {plan.describe()} "
                        "发送，请稍候……"
                    )
                )
                outgoing = self._build_outgoing_chain(collected)
                succeeded, failures = await self._execute_plan(
                    content_event, plan, outgoing
                )
                summary = f"群发完成：成功 {succeeded}/{plan.total}"
                if failures:
                    preview = "；".join(failures[:10])
                    if len(failures) > 10:
                        preview += f"；另有 {len(failures) - 10} 个失败目标"
                    summary += f"\n失败：{preview}"
                await content_event.send(content_event.plain_result(summary))
                return

            message_chain = self._copy_message_chain(content_event)
            if not message_chain.chain:
                await content_event.send(
                    content_event.plain_result(
                        "消息内容为空，请重新发送，或发送“发送”/“取消”。"
                    )
                )
                controller.keep(timeout=WAIT_TIMEOUT_SECONDS, reset_timeout=True)
                return

            collected.append(
                CollectedMessage(
                    chain=message_chain,
                    sender_id=content_event.get_sender_id(),
                    sender_name=(
                        content_event.get_sender_name()
                        or content_event.get_sender_id()
                        or "管理员"
                    ),
                )
            )
            await content_event.send(
                content_event.plain_result(
                    f"已收集第 {len(collected)} 条消息。继续发送内容，"
                    "或发送“发送”开始群发、“取消”终止。"
                )
            )
            controller.keep(timeout=WAIT_TIMEOUT_SECONDS, reset_timeout=True)

        try:
            await wait_for_content(event, session_filter=ExactSessionFilter())
        except TimeoutError:
            yield event.plain_result("等待群发内容超时，操作已取消。")
        except Exception as exc:  # noqa: BLE001 - 会话处理需向管理员报告异常
            logger.exception("[%s] 群发会话异常", PLUGIN_NAME)
            yield event.plain_result(f"群发失败：{exc}")
        finally:
            event.stop_event()

    async def _build_plan(
        self,
        event: AiocqhttpMessageEvent,
        target: str,
        value: str,
    ) -> SendPlan:
        if target == "all" and not value:
            groups, friends = await self._get_contacts(event)
            return self._ensure_nonempty(SendPlan(tuple(groups), tuple(friends)))

        if target in {"group", "friend"}:
            if not value:
                raise ValueError(f"缺少 {target} 的目标，应该是 all 或数字 ID")
            if value == "all":
                groups, friends = await self._get_contacts(event)
                plan = (
                    SendPlan(groups=tuple(groups))
                    if target == "group"
                    else SendPlan(friends=tuple(friends))
                )
                return self._ensure_nonempty(plan)
            if not value.isdigit():
                raise ValueError("群号和 QQ 号必须为纯数字")
            return (
                SendPlan(groups=(value,))
                if target == "group"
                else SendPlan(friends=(value,))
            )

        if value:
            raise ValueError("无法识别多余参数")
        if not target.isdigit():
            raise ValueError("目标必须是群号、QQ 号、group all、friend all 或 all")

        groups, friends = await self._get_contacts(event)
        in_groups = target in groups
        in_friends = target in friends
        if in_groups and in_friends:
            raise ValueError(
                "该 ID 同时存在于群列表和好友列表，请使用 group <ID> 或 friend <ID>"
            )
        if in_groups:
            return SendPlan(groups=(target,))
        if in_friends:
            return SendPlan(friends=(target,))
        raise ValueError("该 ID 不在当前 Bot 的群列表或好友列表中")

    async def _get_contacts(
        self, event: AiocqhttpMessageEvent
    ) -> tuple[list[str], list[str]]:
        routing = self._routing_params(event)
        group_items, friend_items = await asyncio.gather(
            event.bot.call_action("get_group_list", **routing),
            event.bot.call_action("get_friend_list", **routing),
        )
        groups = self._extract_ids(group_items, "group_id")
        friends = self._extract_ids(friend_items, "user_id")
        return groups, friends

    @staticmethod
    def _extract_ids(items: Any, key: str) -> list[str]:
        if not isinstance(items, list):
            raise TypeError(f"OneBot 返回的 {key} 列表格式不正确")
        ids = {
            str(item[key])
            for item in items
            if isinstance(item, dict) and item.get(key) is not None
        }
        return sorted(ids, key=lambda item: (len(item), item))

    @staticmethod
    def _ensure_nonempty(plan: SendPlan) -> SendPlan:
        if not plan.total:
            raise ValueError("没有找到可发送的目标")
        return plan

    async def _execute_plan(
        self,
        event: AstrMessageEvent,
        plan: SendPlan,
        source_chain: MessageChain,
    ) -> tuple[int, list[str]]:
        bot = getattr(event, "bot", None)
        if bot is None:
            raise RuntimeError("当前事件没有可用的 aiocqhttp Bot 客户端")

        raw_event = getattr(event.message_obj, "raw_message", None)
        succeeded = 0
        failures: list[str] = []
        targets: list[tuple[TargetKind, str]] = [
            *(("group", group_id) for group_id in plan.groups),
            *(("friend", user_id) for user_id in plan.friends),
        ]

        for index, (kind, target_id) in enumerate(targets):
            try:
                await AiocqhttpMessageEvent.send_message(
                    bot=bot,
                    message_chain=source_chain.derive(list(source_chain.chain)),
                    event=raw_event,
                    is_group=kind == "group",
                    session_id=target_id,
                )
                succeeded += 1
            except ActionFailed as exc:
                label = "群" if kind == "group" else "好友"
                reason = "被禁言，发送失败" if self._is_muted_error(exc) else "发送失败"
                failures.append(f"{label} {target_id}（{reason}）")
                logger.warning(
                    "[%s] 发送到 %s %s 失败: %s",
                    PLUGIN_NAME,
                    kind,
                    target_id,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001 - 单目标失败不应中断群发
                label = "群" if kind == "group" else "好友"
                failures.append(f"{label} {target_id}（发送失败）")
                logger.warning(
                    "[%s] 发送到 %s %s 失败: %s",
                    PLUGIN_NAME,
                    kind,
                    target_id,
                    exc,
                )
            if index + 1 < len(targets):
                await asyncio.sleep(
                    random.uniform(
                        MIN_SEND_INTERVAL_SECONDS,
                        MAX_SEND_INTERVAL_SECONDS,
                    )
                )

        return succeeded, failures

    @staticmethod
    def _is_muted_error(exc: ActionFailed) -> bool:
        detail = json.dumps(exc.result, ensure_ascii=False, default=str).lower()
        muted_markers = (
            "禁言",
            "muted",
            "mute",
            "group ban",
            "banned from speaking",
        )
        if any(marker in detail for marker in muted_markers):
            return True

        # NapCat / QQ NT 在 Bot 被禁言时常返回：外层 retcode=1200，
        # NodeIKernelMsgService/sendMsg 的内部 EventRet result=120。
        return (
            exc.retcode == 1200
            and "nodeikernelmsgservice/sendmsg" in detail
            and re.search(r"\"result\"\s*:\s*120\b", detail) is not None
        )

    @staticmethod
    def _copy_message_chain(event: AstrMessageEvent) -> MessageChain:
        chain = MessageChain(chain=list(event.get_messages()))
        result = event.get_result()
        if result is not None:
            chain.use_t2i_ = result.use_t2i_
            chain.use_markdown_ = result.use_markdown_
            chain.type = result.type
        return chain

    @staticmethod
    def _get_instruction(event: AstrMessageEvent) -> str:
        components = event.get_messages()
        if len(components) != 1 or not isinstance(components[0], Plain):
            return ""
        return components[0].text.strip().lower()

    @staticmethod
    def _build_outgoing_chain(messages: list[CollectedMessage]) -> MessageChain:
        if len(messages) == 1:
            source = messages[0].chain
            return source.derive(list(source.chain))
        nodes = [
            Node(
                content=list(message.chain.chain),
                uin=message.sender_id or "0",
                name=message.sender_name,
            )
            for message in messages
        ]
        return MessageChain(chain=[Nodes(nodes)])

    @staticmethod
    def _routing_params(event: AstrMessageEvent) -> dict[str, int]:
        self_id = event.get_self_id()
        return {"self_id": int(self_id)} if self_id and self_id.isdigit() else {}
