from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import aiohttp
from disnake.ext import tasks
from loguru import logger

from .enums import NodeStatus
from .exceptions import AuthorizationFailedException, NodeException
from .payloads import *
from .tracks import Playable

if TYPE_CHECKING:
    from .node import Node
    from .player import Player
    from .types.request import UpdateSessionRequest
    from .types.state import PlayerState
    from .types.websocket import (
        TrackExceptionPayload, 
        WebsocketOP, 
        ReadyOP, 
        PlayerUpdateOP, 
        TrackStartEvent,
        TrackEndEvent, 
        TrackExceptionEvent, 
        TrackStuckEvent, 
        WebsocketClosedEvent
    )

class Websocket:
    def __init__(self, *, node: Node) -> None:
        self.node: Node = node

        self._now_retries: int = 0  # Количество попыток подключения
        self.retries: int = 10  # Максимальное количество попыток подключения

        self.socket: aiohttp.ClientWebSocketResponse | None = None  # WebSocket-соединение
        self.keep_alive_task: asyncio.Task[None] | None = None  # Задача для поддержания соединения

    PATTERN = "v4/websocket"  # Паттерн для WebSocket

    @property
    def headers(self) -> dict[str, str]:
        assert self.node.client is not None
        assert self.node.client.user is not None

        data = {
            "Authorization": self.node.password,  # Пароль для авторизации
            "User-Id": str(self.node.client.user.id),  # Идентификатор пользователя
            "Client-Name": f"MaseLink/1.0.0",  # Имя клиента
        }

        if self.node.session_id:
            data["Session-Id"] = self.node.session_id  # Идентификатор сессии, если он есть

        return data

    def is_connected(self) -> bool:
        return self.socket is not None and not self.socket.closed  # Проверка состояния соединения

    async def _update_node(self) -> None:
        if self.node.resume_timeout > 0:
            udata: UpdateSessionRequest = {"resuming": True, "timeout": self.node.resume_timeout}
            await self.node.update_session(data=udata)  # Обновление сессии

    @tasks.loop(seconds=30)
    async def _connect(self) -> None:
        uri: str = f"{self.node.uri.removesuffix('/')}/" + self.PATTERN  # URL для подключения
        try:
            self.socket = await self.node._session.ws_connect(
                url=uri, heartbeat=self.node.heartbeat, headers=self.headers
            )
        except Exception as e:
            if isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 401:
                await self.cleanup()  # Очистка при ошибке авторизации
                raise AuthorizationFailedException from e
            elif isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 404:
                await self.cleanup()  # Очистка при ошибке 404
                raise NodeException from e
            elif (
                    isinstance(e, aiohttp.ClientConnectorError)
                    or isinstance(e, ConnectionRefusedError)
                    or isinstance(e, aiohttp.ClientOSError)
            ):
                pass  # Ошибки подключения игнорируются
            else:
                logger.warning(f"Узел выдал ошибку: {e} ({type(e).__name__})")

        if self.is_connected():
            logger.info(
                f"{self.node.identifier} "
                f"успешно подключился/переподключился к Lavalink после "
                f'"{self._now_retries + 1}" попытки подключения.'
            )

            self._now_retries = 0  # Сбрасываем счетчик попыток
            self.keep_alive.start()  # Запускаем задачу поддержания соединения
            return self._connect.cancel()

        if self._now_retries > self.retries:
            logger.debug(
                f"{self.node.identifier} не удалось успешно подключиться/переподключиться к Lavalink после "
                f'"{self._now_retries}" попыток. Этот узел исчерпал количество попыток.'
            )

            await self.cleanup()  # Очищаем ресурсы
            return self._connect.cancel()

        self._now_retries += 1  # Увеличиваем счетчик попыток
        logger.debug(f'{self.node!r} повторная попытка подключения WebSocket через "30" секунд.')

    def connect(self) -> None:
        self.node._status = NodeStatus.CONNECTING  # Устанавливаем статус подключения

        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()  # Отменяем задачу поддержания соединения
            except Exception as e:
                _ = e

        self._connect.start()  # Запускаем процесс подключения

    @tasks.loop()
    async def keep_alive(self) -> None:
        message: aiohttp.WSMessage = await self.socket.receive()  # Получаем сообщение от WebSocket

        if message.type in (
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
        ):
            self.dispatch("connection_lost", self.node.client.voice_clients)  # Сообщаем о потере соединения
            self.connect()  # Повторное подключение
            return self.keep_alive.cancel()

        if message.data:
            data: WebsocketOP = message.json()  # Декодируем сообщение

            if data["op"] == "ready":
                data: ReadyOP
                resumed: bool = data["resumed"]
                session_id: str = data["sessionId"]

                self.node._status = NodeStatus.CONNECTED  # Устанавливаем статус подключенного узла
                self.node._session_id = session_id  # Сохраняем идентификатор сессии

                await self._update_node()  # Обновляем узел

                ready_payload: NodeReadyEventPayload = NodeReadyEventPayload(
                    node=self.node, resumed=resumed, session_id=session_id
                )
                self.dispatch("node_ready", ready_payload)  # Сообщаем о готовности узла

            elif data["op"] == "playerUpdate":
                data: PlayerUpdateOP
                player: Player | None = self.get_player(data["guildId"])  # Получаем игрока
                state: PlayerState = data["state"]

                payload: PlayerUpdateEventPayload = PlayerUpdateEventPayload(player=player, state=state)
                self.dispatch("player_update", payload)  # Сообщаем об обновлении игрока

                if player:
                    asyncio.create_task(player.update_event(payload))  # Обновляем событие игрока

            elif data["op"] == "stats":
                payload: StatsEventPayload = StatsEventPayload(data=data)
                self.node._total_player_count = payload.players  # Обновляем общее количество игроков
                self.dispatch("stats_update", payload)  # Сообщаем об обновлении статистики

            elif data["op"] == "event":
                player: Player | None = self.get_player(data["guildId"])

                if data["type"] == "TrackStartEvent":
                    data: TrackStartEvent

                    track: Playable = Playable(data["track"])  # Создаем трек

                    payload: TrackStartEventPayload = TrackStartEventPayload(player=player, track=track)
                    self.dispatch("track_start", payload)  # Сообщаем о начале воспроизведения трека

                    if player:
                        player.track_start()  # Запускаем трек у игрока

                elif data["type"] == "TrackEndEvent":
                    data: TrackEndEvent

                    track: Playable = Playable(data["track"])
                    reason: str = data["reason"]

                    if player and reason != "replaced":
                        player._current = None  # Удаляем текущий трек

                    payload: TrackEndEventPayload = TrackEndEventPayload(player=player, track=track, reason=reason)
                    self.dispatch("track_end", payload)  # Сообщаем о завершении трека

                    if player:
                        asyncio.create_task(player.auto_play_event(payload))  # Запускаем событие автопроигрывания

                elif data["type"] == "TrackExceptionEvent":
                    data: TrackExceptionEvent

                    track: Playable = Playable(data["track"])
                    exception: TrackExceptionPayload = data["exception"]

                    payload: TrackExceptionEventPayload = TrackExceptionEventPayload(
                        player=player, track=track, exception=exception
                    )
                    self.dispatch("track_exception", payload)  # Сообщаем о возникновении исключения трека

                elif data["type"] == "TrackStuckEvent":
                    data: TrackStuckEvent

                    track: Playable = Playable(data["track"])
                    threshold: int = data["thresholdMs"]

                    payload: TrackStuckEventPayload = TrackStuckEventPayload(
                        player=player, track=track, threshold=threshold
                    )
                    self.dispatch("track_stuck", payload)  # Сообщаем о застревании трека

                elif data["type"] == "WebSocketClosedEvent":
                    data: WebsocketClosedEvent

                    code: int = data["code"]
                    reason: str = data["reason"]
                    by_remote: bool = data["byRemote"]

                    payload: WebsocketClosedEventPayload = WebsocketClosedEventPayload(
                        player=player, code=code, reason=reason, by_remote=by_remote
                    )
                    self.dispatch("websocket_closed", payload)  # Сообщаем о закрытии WebSocket

                else:
                    other_payload: ExtraEventPayload = ExtraEventPayload(node=self.node, player=player, data=data)
                    self.dispatch("extra_event", other_payload)  # Сообщаем о дополнительном событии
            else:
                logger.warning(
                    f"'Получено неизвестное OP от Lavalink '{data['op']}'. Игнорируем."
                )

    def get_player(self, guild_id: str | int) -> Player | None:
        return self.node.get_player(int(guild_id))  # Получаем игрока по идентификатору гильдии

    def dispatch(self, event: str, /, *args: Any, **kwargs: Any) -> None:
        assert self.node.client is not None

        self.node.client.dispatch(f"Mase_{event}", *args, **kwargs)  # Рассылаем событие

    async def cleanup(self) -> None:
        if self.socket:
            try:
                await self.socket.close()  # Закрываем сокет
            except Exception as e:
                _ = e

        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()  # Отменяем задачу поддержания соединения
            except Exception as e:
                _ = e

        self.node._status = NodeStatus.DISCONNECTED  # Устанавливаем статус отключенного узла
        self.node._session_id = None  # Сбрасываем идентификатор сессии
        self.node._players = {}  # Очищаем игроков

        self.node._websocket = None  # Очищаем сокет

        logger.info(f"Успешно очищен WebSocket для {self.node!r}")  # Логируем информацию об очистке
