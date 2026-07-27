"""Cliente OPC-UA: se conecta al servidor (PLC simulado) y lee variables.

Muestra dos modos de lectura: polling (sondeo periódico) y suscripción
(el servidor notifica los cambios).
"""

from __future__ import annotations

import asyncio
import logging

from asyncua import Client

logger = logging.getLogger(__name__)

# La misma URL donde el servidor está escuchando.
SERVER_URL = "opc.tcp://localhost:4840/freeopcua/server/"
NAMESPACE_URI = "http://industrial-gateway.demo"


async def read_once() -> None:
    """Se conecta, lee todas las variables una vez, y se desconecta."""
    async with Client(url=SERVER_URL) as client:
        # Obtener el índice de nuestro namespace.
        idx = await client.get_namespace_index(NAMESPACE_URI)

        # Navegar hasta las variables por su path en el árbol.
        # Objects -> Maquina -> [variable]
        temperature = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Temperatura"])
        pressure = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Presion"])
        machine_on = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:EnMarcha"])
        prod_counter = await client.nodes.objects.get_child(
            [f"{idx}:Maquina", f"{idx}:ContadorProduccion"]
        )

        # Leer los valores.
        temp = await temperature.read_value()
        pres = await pressure.read_value()
        is_on = await machine_on.read_value()
        count = await prod_counter.read_value()

        logger.info(
            "Temperatura=%.2f°C | Presión=%.2f bar | En marcha=%s | Producción=%d",
            temp,
            pres,
            is_on,
            count,
        )


async def poll_loop(interval: float = 2.0) -> None:
    """Lee las variables periódicamente (polling), hasta que se interrumpa."""
    async with Client(url=SERVER_URL) as client:
        idx = await client.get_namespace_index(NAMESPACE_URI)

        temperature = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Temperatura"])
        pressure = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Presion"])

        logger.info("Iniciando polling cada %.1f segundos...", interval)
        while True:
            temp = await temperature.read_value()
            pres = await pressure.read_value()
            logger.info("Temperatura=%.2f°C | Presión=%.2f bar", temp, pres)
            await asyncio.sleep(interval)


"""
def main() -> None:
    Punto de entrada: corre el polling loop.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    try:
        asyncio.run(poll_loop())
    except KeyboardInterrupt:
        logger.info("Cliente detenido por el usuario")
    except ConnectionError:
        logger.error("No se pudo conectar al servidor. ¿Está corriendo?")
"""


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    try:
        asyncio.run(subscribe_loop())  # cambiado de poll_loop a subscribe_loop
    except KeyboardInterrupt:
        logger.info("Cliente detenido por el usuario")
    except ConnectionError:
        logger.error("No se pudo conectar al servidor. ¿Está corriendo?")


class SubscriptionHandler:
    """Maneja las notificaciones de cambio de las variables suscritas.

    OPC-UA llama a datachange_notification automáticamente cada vez que
    una variable suscrita cambia de valor.
    """

    def datachange_notification(self, node: object, value: object, data: object) -> None:
        """Se ejecuta cuando una variable suscrita cambia.

        Args:
            node: el nodo que cambió.
            value: el nuevo valor.
            data: metadatos del cambio (timestamp, calidad, etc.).
        """
        logger.info("Cambio detectado: %s = %s", node, value)


async def subscribe_loop() -> None:
    """Se suscribe a las variables y reacciona a sus cambios en tiempo real."""
    async with Client(url=SERVER_URL) as client:
        idx = await client.get_namespace_index(NAMESPACE_URI)

        temperature = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Temperatura"])
        pressure = await client.nodes.objects.get_child([f"{idx}:Maquina", f"{idx}:Presion"])

        # Crear la suscripción con nuestro handler.
        handler = SubscriptionHandler()
        # 500 = intervalo de muestreo en ms (cada cuánto el servidor chequea cambios).
        subscription = await client.create_subscription(500, handler)

        # Suscribirse a los cambios de datos de ambas variables.
        await subscription.subscribe_data_change(temperature)
        await subscription.subscribe_data_change(pressure)

        logger.info("Suscripción activa. Esperando cambios... (Ctrl+C para salir)")

        # Mantener el cliente vivo para seguir recibiendo notificaciones.
        try:
            while True:
                await asyncio.sleep(1.0)
        finally:
            await subscription.delete()


if __name__ == "__main__":
    main()
