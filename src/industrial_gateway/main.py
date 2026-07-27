"""Orquestador: une el gateway OPC-UA con el publisher MQTT vía una cola."""

from __future__ import annotations

import asyncio
import logging
import sys

from industrial_gateway.gateway import DataPoint, GatewayConfig, OpcuaGateway
from industrial_gateway.mqtt_publisher import MqttConfig, publish_from_queue

logger = logging.getLogger(__name__)


async def run() -> None:
    """Corre el gateway y el publisher en paralelo, comunicados por una cola."""
    # maxsize acota el buffer: si MQTT no da abasto, preferimos descartar
    # datos viejos antes que consumir memoria sin límite.
    queue: asyncio.Queue[DataPoint] = asyncio.Queue(maxsize=1000)

    def enqueue(point: DataPoint) -> None:
        """Callback del gateway: encola sin bloquear."""
        try:
            queue.put_nowait(point)
        except asyncio.QueueFull:
            logger.warning("Cola llena, se descarta dato de %s", point.variable)

    gateway = OpcuaGateway(GatewayConfig(), on_data=enqueue)

    # Ambas tareas corren concurrentemente en el mismo event loop.
    await asyncio.gather(
        gateway.run(),
        publish_from_queue(queue, MqttConfig()),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    # Silenciar el logging interno de asyncua (muy verboso).
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    # En Windows, el ProactorEventLoop (default) no soporta add_reader/add_writer,
    # que paho-mqtt necesita. El SelectorEventLoop sí.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Detenido por el usuario")


if __name__ == "__main__":
    main()
