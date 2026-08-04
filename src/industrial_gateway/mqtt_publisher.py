"""Publicador MQTT: consume datos del gateway y los envía a un broker."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
from dataclasses import dataclass

import aiomqtt

from industrial_gateway.gateway import DataPoint
from industrial_gateway.retry import next_backoff

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MqttConfig:
    """Configuración de la conexión al broker MQTT."""

    host: str = "localhost"
    port: int = 1883
    base_topic: str = "planta/maquina1"
    reconnect_initial_sec: float = 1.0
    reconnect_max_sec: float = 30.0


def build_topic(base_topic: str, variable: str) -> str:
    """Arma el topic de una variable.

    Ej: ("planta/maquina1", "Temperatura") -> "planta/maquina1/temperatura"
    """
    return f"{base_topic}/{variable.lower()}"


def build_payload(point: DataPoint) -> str:
    """Serializa un dato como JSON con timestamp UTC."""
    return json.dumps(
        {
            "variable": point.variable,
            "value": point.value,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }
    )


async def publish_from_queue(queue: asyncio.Queue[DataPoint], config: MqttConfig) -> None:
    """Consume la cola y publica en MQTT, reconectando con backoff si falla."""
    backoff = config.reconnect_initial_sec

    while True:
        try:
            async with aiomqtt.Client(hostname=config.host, port=config.port) as client:
                logger.info("Publisher MQTT conectado a %s:%d", config.host, config.port)
                backoff = config.reconnect_initial_sec

                while True:
                    point = await queue.get()
                    topic = build_topic(config.base_topic, point.variable)
                    payload = build_payload(point)
                    await client.publish(topic, payload)
                    logger.info("MQTT → %s : %s", topic, point.value)

        except aiomqtt.MqttError as exc:
            logger.warning("MQTT desconectado (%s). Reintentando en %.1f s...", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = next_backoff(backoff, config.reconnect_max_sec)
