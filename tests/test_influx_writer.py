"""Tests de la conversión de mensajes MQTT a puntos de InfluxDB."""

from __future__ import annotations

import json

import pytest

from industrial_gateway.influx_writer import build_point, parse_machine_from_topic

MEASUREMENT = "plc_data"


# --- parse_machine_from_topic ---


@pytest.mark.parametrize(
    "topic,esperado",
    [
        ("planta/maquina1/temperatura", "maquina1"),
        ("planta/maquina2/presion", "maquina2"),
        ("fabrica/linea3/caudal", "linea3"),
    ],
)
def test_parse_machine_extrae_el_segundo_nivel(topic: str, esperado: str) -> None:
    """La máquina es el segundo segmento del topic."""
    assert parse_machine_from_topic(topic) == esperado


@pytest.mark.parametrize("topic", ["planta", "", "sinbarras"])
def test_parse_machine_topic_sin_formato(topic: str) -> None:
    """Un topic sin suficientes niveles devuelve 'desconocida'."""
    assert parse_machine_from_topic(topic) == "desconocida"


# --- build_point: casos válidos ---


def test_build_point_incluye_measurement_y_tags() -> None:
    """El punto lleva el measurement y los tags de variable y máquina."""
    payload = json.dumps({"variable": "Temperatura", "value": 25.5})
    point = build_point("planta/maquina1/temperatura", payload, MEASUREMENT)

    assert point is not None
    linea = point.to_line_protocol()
    assert linea.startswith("plc_data")
    assert "variable=Temperatura" in linea
    assert "machine=maquina1" in linea


def test_build_point_guarda_el_valor_como_field() -> None:
    """El valor numérico se guarda en el field 'value'."""
    payload = json.dumps({"variable": "Presion", "value": 5.25})
    point = build_point("planta/maquina1/presion", payload, MEASUREMENT)

    assert point is not None
    assert "value=5.25" in point.to_line_protocol()


@pytest.mark.parametrize(
    "valor,esperado",
    [
        (True, "value=1"),
        (False, "value=0"),
    ],
)
def test_build_point_convierte_booleanos_a_numeros(
    valor: bool, esperado: str
) -> None:
    """Los booleanos se guardan como 1/0 para poder graficarlos."""
    payload = json.dumps({"variable": "EnMarcha", "value": valor})
    point = build_point("planta/maquina1/enmarcha", payload, MEASUREMENT)

    assert point is not None
    assert esperado in point.to_line_protocol()


def test_build_point_usa_el_timestamp_del_payload() -> None:
    """Si el payload trae timestamp, se usa ese y no la hora actual."""
    payload = json.dumps(
        {
            "variable": "Temperatura",
            "value": 20.0,
            "timestamp": "2025-01-01T00:00:00+00:00",
        }
    )
    point = build_point("planta/maquina1/temperatura", payload, MEASUREMENT)

    assert point is not None
    # 2025-01-01T00:00:00Z en nanosegundos desde epoch.
    assert "1735689600000000000" in point.to_line_protocol()


def test_build_point_acepta_timestamp_invalido_sin_fallar() -> None:
    """Un timestamp mal formado no rompe: se usa la hora actual."""
    payload = json.dumps(
        {"variable": "Temperatura", "value": 20.0, "timestamp": "ayer a la tarde"}
    )
    point = build_point("planta/maquina1/temperatura", payload, MEASUREMENT)

    assert point is not None  # el punto se construye igual


# --- build_point: casos inválidos (devuelven None) ---


def test_build_point_rechaza_json_invalido() -> None:
    """Un payload que no es JSON devuelve None en vez de explotar."""
    assert build_point("planta/maquina1/x", "{esto no es json", MEASUREMENT) is None


@pytest.mark.parametrize(
    "payload",
    [
        json.dumps({"value": 25.0}),  # falta 'variable'
        json.dumps({"variable": "Temperatura"}),  # falta 'value'
        json.dumps({}),  # faltan ambos
    ],
)
def test_build_point_rechaza_payload_incompleto(payload: str) -> None:
    """Sin 'variable' o sin 'value', el mensaje se descarta."""
    assert build_point("planta/maquina1/x", payload, MEASUREMENT) is None


@pytest.mark.parametrize("valor", ["texto", [1, 2, 3], {"a": 1}])
def test_build_point_rechaza_valores_no_numericos(valor: object) -> None:
    """Un valor que no es número se descarta (InfluxDB guarda números)."""
    payload = json.dumps({"variable": "X", "value": valor})
    assert build_point("planta/maquina1/x", payload, MEASUREMENT) is None
