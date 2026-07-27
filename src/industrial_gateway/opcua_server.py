"""Servidor OPC-UA simulado: actúa como un PLC exponiendo variables de proceso.

Simula una máquina industrial con temperatura, presión, estado y un contador
de producción que evolucionan con el tiempo. Sirve para desarrollar y probar
el gateway sin necesidad de un PLC físico.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random

from asyncua import Server

logger = logging.getLogger(__name__)

# Endpoint donde el servidor escucha. 4840 es el puerto estándar de OPC-UA.
ENDPOINT = "opc.tcp://0.0.0.0:4840/freeopcua/server/"
# Namespace propio para nuestras variables.
NAMESPACE_URI = "http://industrial-gateway.demo"


async def run_server() -> None:
    """Levanta el servidor OPC-UA y actualiza las variables en un loop infinito."""
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name("Industrial Gateway - PLC Simulado")

    # Registrar nuestro namespace y obtener su índice.
    idx = await server.register_namespace(NAMESPACE_URI)

    # Crear un objeto "Maquina" que agrupa las variables (como un DB en Siemens).
    machine = await server.nodes.objects.add_object(idx, "Maquina")

    # Crear las variables. add_variable(nodeid, nombre, valor_inicial).
    temperature = await machine.add_variable(idx, "Temperatura", 25.0)
    pressure = await machine.add_variable(idx, "Presion", 5.0)
    machine_on = await machine.add_variable(idx, "EnMarcha", True)
    prod_counter = await machine.add_variable(idx, "ContadorProduccion", 0)

    # Hacer las variables escribibles por clientes (opcional, pero útil).
    await temperature.set_writable()
    await pressure.set_writable()
    await machine_on.set_writable()

    logger.info("Servidor OPC-UA iniciado en %s", ENDPOINT)

    counter = 0
    async with server:
        # Loop de simulación: actualiza las variables cada segundo.
        while True:
            await asyncio.sleep(1.0)
            counter += 1

            # Temperatura: oscila alrededor de 25°C con una onda + ruido.
            temp = 25.0 + 5.0 * math.sin(counter / 10) + random.uniform(-0.5, 0.5)
            await temperature.write_value(round(temp, 2))

            # Presión: oscila alrededor de 5 bar.
            pres = 5.0 + 1.0 * math.sin(counter / 7) + random.uniform(-0.1, 0.1)
            await pressure.write_value(round(pres, 2))

            # Estado: la máquina está en marcha la mayor parte del tiempo.
            is_on = random.random() > 0.05  # 95% del tiempo encendida
            await machine_on.write_value(is_on)

            # Contador: incrementa solo si la máquina está en marcha.
            if is_on:
                await prod_counter.write_value(counter)


def main() -> None:
    """Punto de entrada: configura logging y corre el servidor."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario")


if __name__ == "__main__":
    main()
