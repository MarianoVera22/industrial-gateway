"""Consumidor MQTT que persiste los datos en InfluxDB.

Se suscribe a los topics del gateway y escribe cada mensaje como un punto
en una base de series temporales. Proceso independiente del gateway:
ambos solo comparten el broker MQTT.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import sys
from dataclasses import dataclass

import aiomqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

from industrial_gateway.retry import next_backoff

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InfluxConfig:
    """Configuración de la conexión a InfluxDB."""

    url: str = "http://localhost:8086"
    token: str = "mi-token-de-desarrollo"
    org: str = "planta"
    bucket: str = "industrial"
    measurement: str = "plc_data"


@dataclass(frozen=True, slots=True)
class MqttSubConfig:
    """Configuración de la suscripción MQTT."""

    host: str = "localhost"
    port: int = 1883
    topic_filter: str = "planta/+/+"
    reconnect_initial_sec: float = 1.0
    reconnect_max_sec: float = 30.0


def parse_machine_from_topic(topic: str) -> str:
    """Extrae el nombre de la máquina de un topic.

    Ej: "planta/maquina1/temperatura" -> "maquina1"
    Si el topic no tiene el formato esperado, devuelve "desconocida".
    """
    parts = topic.split("/")
    return parts[1] if len(parts) >= 2 else "desconocida"


def build_point(topic: str, payload: str, measurement: str) -> Point | None:
    """Convierte un mensaje MQTT en un punto de InfluxDB.

    Args:
        topic: el topic del mensaje.
        payload: el JSON publicado por el gateway.
        measurement: nombre del measurement en InfluxDB.

    Returns:
        Un Point listo para escribir, o None si el payload no es válido
        o el valor no es numérico.
    """
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("Payload no es JSON válido, se descarta: %r", payload[:80])
        return None

    variable = data.get("variable")
    value = data.get("value")
    if variable is None or value is None:
        logger.warning("Payload sin 'variable' o 'value', se descarta")
        return None

    # InfluxDB guarda números; los booleanos los convertimos a 0/1
    # para poder graficarlos junto al resto.
    if isinstance(value, bool):
        value = int(value)
    elif not isinstance(value, (int, float)):
        logger.warning("Valor no numérico para %s, se descarta: %r", variable, value)
        return None

    point = (
        Point(measurement)
        .tag("variable", str(variable))
        .tag("machine", parse_machine_from_topic(topic))
        .field("value", float(value))
    )

    # Usar el timestamp del gateway si viene; si no, el actual.
    timestamp = data.get("timestamp")
    if isinstance(timestamp, str):
        try:
            point = point.time(datetime.datetime.fromisoformat(timestamp))
        except ValueError:
            logger.warning("Timestamp inválido, se usa el actual: %r", timestamp)

    return point


async def consume_and_store(
    mqtt_config: MqttSubConfig, influx_config: InfluxConfig
) -> None:
    """Se suscribe a MQTT y escribe cada mensaje en InfluxDB."""
    backoff = mqtt_config.reconnect_initial_sec

    with InfluxDBClient(
        url=influx_config.url,
        token=influx_config.token,
        org=influx_config.org,
    ) as influx:
        write_api = influx.write_api(write_options=SYNCHRONOUS)
        logger.info("Conectado a InfluxDB en %s", influx_config.url)

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=mqtt_config.host, port=mqtt_config.port
                ) as client:
                    await client.subscribe(mqtt_config.topic_filter)
                    logger.info(
                        "Suscrito a '%s'. Guardando en bucket '%s'.",
                        mqtt_config.topic_filter,
                        influx_config.bucket,
                    )
                    backoff = mqtt_config.reconnect_initial_sec

                    async for message in client.messages:
                        topic = str(message.topic)
                        raw = message.payload
                        payload = (
                            raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                        )

                        point = build_point(topic, payload, influx_config.measurement)
                        if point is None:
                            continue

                        try:
                            write_api.write(bucket=influx_config.bucket, record=point)
                            logger.info("InfluxDB <- %s", topic)
                        except (InfluxDBError, OSError) as exc:
                            # La base no responde: lo registramos y seguimos
                            # escuchando. No matamos el consumidor por un
                            # fallo de escritura.
                            logger.warning(
                                "No se pudo escribir en InfluxDB (%s): %s",
                                type(exc).__name__,
                                exc,
                            )

            except aiomqtt.MqttError as exc:
                logger.warning(
                    "MQTT desconectado (%s). Reintentando en %.1f s...", exc, backoff
                )
                await asyncio.sleep(backoff)
                backoff = next_backoff(backoff, mqtt_config.reconnect_max_sec)


def main() -> None:
    """Punto de entrada del consumidor."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    # En Windows, el ProactorEventLoop (default) no soporta add_reader/
    # add_writer, que paho-mqtt necesita. El SelectorEventLoop sí.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(consume_and_store(MqttSubConfig(), InfluxConfig()))
    except KeyboardInterrupt:
        logger.info("Consumidor detenido por el usuario")


if __name__ == "__main__":
    main()
