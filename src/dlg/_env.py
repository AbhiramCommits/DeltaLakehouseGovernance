"""Environment pinning for a self-contained local Spark.

Spark is extremely sensitive to ``SPARK_HOME`` / ``JAVA_HOME`` / ``PYSPARK_*``
environment variables.  Developer machines (and CI images) frequently export a
Homebrew ``apache-spark`` install, a Jupyter driver hook, etc.  Those leak into
PySpark and produce confusing failures such as
``UnsupportedClassVersionError`` or "Java gateway process exited".

This module normalises the environment *before* PySpark is imported so that the
pip-installed ``pyspark==3.5.1`` distribution and a compatible JDK are always
used.  It is imported for its side effect by :mod:`dlg` (the package
``__init__``), which runs before any submodule can import PySpark.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

__all__ = ["configure_env", "find_java_home", "SUPPORTED_JAVA_MAJORS"]

#: Spark 3.5 supports Java 8, 11 and 17.  Newer JDKs are rejected by the launcher.
SUPPORTED_JAVA_MAJORS = (17, 11, 8)

# Common install locations probed when JAVA_HOME is unset or incompatible.
_JAVA_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
    "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
    "/usr/lib/jvm/java-17-openjdk-amd64",
    "/usr/lib/jvm/java-11-openjdk-amd64",
    "/usr/lib/jvm/temurin-17-jdk-amd64",
    "/usr/lib/jvm/temurin-11-jdk-amd64",
)

_configured = False


def _major_version(java_home: str) -> int | None:
    """Best-effort read of the major Java version for ``java_home``."""
    release = Path(java_home) / "release"
    try:
        for line in release.read_text().splitlines():
            if line.startswith("JAVA_VERSION="):
                raw = line.split("=", 1)[1].strip().strip('"')
                head = raw.split(".")[0]
                if head == "1":  # 1.8 style version string
                    head = raw.split(".")[1]
                return int(head)
    except (OSError, ValueError, IndexError):
        return None
    return None


def find_java_home() -> str | None:
    """Return a Spark-compatible ``JAVA_HOME``, or ``None`` if none is found."""
    existing = os.environ.get("JAVA_HOME")
    if existing:
        major = _major_version(existing)
        # Accept an unknown-but-present JAVA_HOME rather than failing outright.
        if major is None or major in SUPPORTED_JAVA_MAJORS:
            return existing
    for candidate in _JAVA_CANDIDATES:
        if Path(candidate, "bin", "java").exists():
            return candidate
    return existing


def configure_env() -> None:
    """Pin ``SPARK_HOME``/``JAVA_HOME``/``PYSPARK_*`` for this interpreter."""
    global _configured
    if _configured:
        return

    # 1. Force the pip-installed PySpark distribution as SPARK_HOME.  A stray
    #    Homebrew SPARK_HOME silently swaps in a different Spark build.
    try:
        import pyspark  # noqa: PLC0415 - deliberately late, after env tweaks

        os.environ["SPARK_HOME"] = os.path.dirname(os.path.abspath(pyspark.__file__))
    except ImportError:  # pragma: no cover - pyspark is a hard dependency
        pass

    # 2. Pin a Spark-compatible JDK.
    java_home = find_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home

    # 3. Make sure the driver and workers use *this* interpreter, not Jupyter.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)

    # 4. Delta's shaded Hadoop client talks to the local FS; keep it quiet.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    _configured = True
