"""Utilidades de reintento: backoff exponencial."""

from __future__ import annotations


def next_backoff(current: float, cap: float, factor: float = 2.0) -> float:
    """Calcula la próxima espera de un backoff exponencial.

    Multiplica la espera actual por el factor, sin superar el tope.

    Args:
        current: espera actual en segundos.
        cap: espera máxima permitida en segundos.
        factor: multiplicador por cada reintento (2.0 = duplicar).

    Returns:
        La próxima espera, acotada por cap.
    """
    return min(current * factor, cap)
