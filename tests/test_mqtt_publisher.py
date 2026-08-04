"""Tests de la construcción de topics y payloads MQTT."""

from __future__ import annotations

import datetime
import json

import pytest

from industrial_gateway.gateway import DataPoint
from industrial_gateway.mqtt_publisher import build_payload, build_topic


@pytest.mark.parametrize(
    "variable,esperado",
    [
        ("Temperatura", "planta/maquina1/temperatura"),
        ("Presion", "planta/maquina1/presion"),
        ("ContadorProduccion", "planta/maquina1/contadorproduccion"),
        ("EnMarcha", "planta/maquina1/enmarcha"),
    ],
)
def test_build_topic_normaliza_a_minusculas(variable: str, esperado: str) -> None:
    """El topic usa el nombre de la variable en minúsculas."""
    assert build_topic("planta/maquina1", variable) == esperado


def test_build_topic_respeta_el_base() -> None:
    """El topic base se antepone tal cual."""
    assert build_topic("fabrica/linea3", "Temperatura") == "fabrica/linea3/temperatura"


def test_payload_es_json_valido() -> None:
    """El payload se puede parsear como JSON."""
    payload = build_payload(DataPoint(variable="Temperatura", value=25.5))
    datos = json.loads(payload)
    assert datos["variable"] == "Temperatura"
    assert datos["value"] == 25.5


def test_payload_incluye_timestamp_utc_parseable() -> None:
    """El timestamp es ISO 8601 con zona horaria UTC."""
    payload = build_payload(DataPoint(variable="Presion", value=5.0))
    datos = json.loads(payload)
    ts = datetime.datetime.fromisoformat(datos["timestamp"])
    assert ts.tzinfo is not None
    assert ts.utcoffset() == datetime.timedelta(0)


@pytest.mark.parametrize("valor", [25.5, 100, True, False, 0])
def test_payload_serializa_distintos_tipos(valor: object) -> None:
    """Los tipos que llegan del PLC (float, int, bool) se serializan bien."""
    payload = build_payload(DataPoint(variable="X", value=valor))
    assert json.loads(payload)["value"] == valor
