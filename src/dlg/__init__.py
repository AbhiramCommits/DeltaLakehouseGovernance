"""Delta Lakehouse Governance - a local-but-real medallion lakehouse on Delta Lake.

Importing this package pins the Spark/JDK environment (see :mod:`dlg._env`)
before any submodule imports PySpark.
"""

from __future__ import annotations

from dlg._env import configure_env

configure_env()

__version__ = "0.1.0"
