"""Gateway OPC-UA robusto: se conecta a un PLC, se suscribe a variables,
y reacciona a los cambios. Se reconecta automáticamente con backoff
exponencial si la conexión falla.

Este es el componente central del sistema: en las siguientes etapas,
los datos recibidos se publicarán por MQTT y se persistirán en InfluxDB.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from asyncua import Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """Configuración del gateway."""

    server_url: str = "opc.tcp://localhost:4840/freeopcua/server/"
    namespace_uri: str = "http://industrial-gateway.demo"
    # Variables a monitorear: (nombre_objeto, nombre_variable).
    variables: tuple[tuple[str, str], ...] = (
        ("Maquina", "Temperatura"),
        ("Maquina", "Presion"),
        ("Maquina", "EnMarcha"),
        ("Maquina", "ContadorProduccion"),
    )
    sampling_interval_ms: int = 500
    # Backoff: espera inicial, factor de multiplicación, tope máximo.
    reconnect_initial_sec: float = 1.0
    reconnect_max_sec: float = 30.0


@dataclass(frozen=True, slots=True)
class DataPoint:
    """Un dato leído del PLC, listo para procesar/enviar."""

    variable: str
    value: object


# Tipo de la función que procesa cada dato recibido.
DataCallback = Callable[[DataPoint], None]


class SubscriptionHandler:
    """Recibe las notificaciones de cambio y las reenvía al callback del gateway."""

    def __init__(self, callback: DataCallback, node_names: dict[str, str]) -> None:
        self._callback = callback
        self._node_names = node_names

    def datachange_notification(self, node: object, value: object, data: object) -> None:
        """Llamado por asyncua cuando una variable suscrita cambia."""
        # Traducir el NodeId a un nombre legible.
        name = self._node_names.get(str(node), str(node))
        self._callback(DataPoint(variable=name, value=value))


class OpcuaGateway:
    """Gateway que conecta con un PLC OPC-UA de forma robusta."""

    def __init__(self, config: GatewayConfig, on_data: DataCallback) -> None:
        """
        Args:
            config: configuración del gateway.
            on_data: función que se llama con cada dato recibido.
        """
        self._config = config
        self._on_data = on_data
        self._running = False

    async def run(self) -> None:
        """Corre el gateway indefinidamente, reconectando con backoff si falla."""
        self._running = True
        backoff = self._config.reconnect_initial_sec

        while self._running:
            try:
                await self._connect_and_subscribe()
                # Si _connect_and_subscribe retorna sin excepción, fue un cierre limpio.
                backoff = self._config.reconnect_initial_sec
            except (ConnectionError, OSError, TimeoutError) as exc:
                logger.warning(
                    "Conexión perdida (%s). Reintentando en %.1f s...",
                    type(exc).__name__,
                    backoff,
                )
                await asyncio.sleep(backoff)
                # Backoff exponencial, limitado por el tope.
                backoff = min(backoff * 2, self._config.reconnect_max_sec)

    def stop(self) -> None:
        """Detiene el gateway tras la iteración en curso."""
        self._running = False

    async def _connect_and_subscribe(self) -> None:
        """Se conecta, se suscribe a las variables y mantiene la conexión viva."""
        async with Client(url=self._config.server_url) as client:
            idx = await client.get_namespace_index(self._config.namespace_uri)

            # Resolver los nodos de las variables configuradas.
            nodes = []
            node_names: dict[str, str] = {}
            for obj_name, var_name in self._config.variables:
                node = await client.nodes.objects.get_child(
                    [f"{idx}:{obj_name}", f"{idx}:{var_name}"]
                )
                nodes.append(node)
                node_names[str(node)] = var_name

            # Crear la suscripción.
            handler = SubscriptionHandler(self._on_data, node_names)
            subscription = await client.create_subscription(
                self._config.sampling_interval_ms, handler
            )
            for node in nodes:
                await subscription.subscribe_data_change(node)

            logger.info(
                "Gateway conectado a %s. Monitoreando %d variables.",
                self._config.server_url,
                len(nodes),
            )

            # Mantener viva la conexión. Si el servidor se cae, alguna
            # operación async lanzará una excepción que sube a run().
            try:
                while self._running:
                    await asyncio.sleep(1.0)
                    # "Ping" para detectar caída del servidor.
                    await client.check_connection()
            finally:
                await subscription.delete()


def main() -> None:
    """Punto de entrada de demostración: imprime cada dato recibido."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    def print_data(point: DataPoint) -> None:
        logger.info("  → %s = %s", point.variable, point.value)

    config = GatewayConfig()
    gateway = OpcuaGateway(config, on_data=print_data)

    try:
        asyncio.run(gateway.run())
    except KeyboardInterrupt:
        logger.info("Gateway detenido por el usuario")


if __name__ == "__main__":
    main()
