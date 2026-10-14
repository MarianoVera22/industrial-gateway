"""Carga de configuración desde variables de entorno.

Los valores se leen del archivo .env (si existe) o del entorno del sistema.
Cada opción tiene un valor por defecto razonable para desarrollo local,
de modo que el proyecto funcione sin configuración previa.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

# Carga el .env de la raíz del proyecto, si existe.
# Las variables ya presentes en el entorno tienen prioridad.
load_dotenv()


def get_str(key: str, default: str) -> str:
    """Lee una variable de entorno como string."""
    return os.getenv(key, default)


def get_int(key: str, default: int) -> int:
    """Lee una variable de entorno como entero.

    Si el valor no es un número válido, usa el default y avisa.
    """
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default
