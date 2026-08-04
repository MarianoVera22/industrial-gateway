"""Tests del cálculo de backoff exponencial."""

from __future__ import annotations

from industrial_gateway.retry import next_backoff


def test_backoff_duplica_por_defecto() -> None:
    """Con factor 2, la espera se duplica en cada paso."""
    assert next_backoff(1.0, cap=30.0) == 2.0
    assert next_backoff(2.0, cap=30.0) == 4.0
    assert next_backoff(4.0, cap=30.0) == 8.0


def test_backoff_respeta_el_tope() -> None:
    """La espera nunca supera el cap."""
    assert next_backoff(20.0, cap=30.0) == 30.0


def test_backoff_se_queda_en_el_tope() -> None:
    """Una vez alcanzado el tope, se mantiene ahí."""
    assert next_backoff(30.0, cap=30.0) == 30.0


def test_backoff_acepta_otro_factor() -> None:
    """El factor de crecimiento es configurable."""
    assert next_backoff(1.0, cap=100.0, factor=3.0) == 3.0


def test_secuencia_completa_converge_al_tope() -> None:
    """Simula varios reintentos: crece y se estabiliza en el tope."""
    espera = 1.0
    secuencia = []
    for _ in range(8):
        secuencia.append(espera)
        espera = next_backoff(espera, cap=30.0)
    assert secuencia == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
