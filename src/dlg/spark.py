"""SparkSession factory.

Builds a local Spark session wired for Delta Lake OSS with the features this
project actually exercises: the Delta catalog, Change Data Feed enabled by
default, schema auto-merge explicitly disabled (so enforcement is real) and a
configurable shuffle partition count.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from dlg.config import Settings, get_settings

__all__ = ["get_spark", "stop_spark", "delta_conf", "active_session"]

_LOCK = threading.Lock()
_SESSION: SparkSession | None = None


def delta_conf(settings: Settings, app_name: str | None = None) -> dict[str, str]:
    """Return the full Spark configuration used by every entry point."""
    return {
        "spark.app.name": app_name or settings.app_name,
        "spark.master": settings.master,
        "spark.driver.memory": settings.driver_memory,
        # --- Delta plumbing -------------------------------------------------
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Change Data Feed on for every new table (needed by silver + streaming).
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        # Schema enforcement: never silently widen a table on write.
        "spark.databricks.delta.schema.autoMerge.enabled": "false",
        # --- determinism ----------------------------------------------------
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(settings.shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # --- local ergonomics ----------------------------------------------
        "spark.sql.warehouse.dir": os.path.abspath(settings.warehouse_dir),
        # Spark 3 defaults to an *in-memory* catalog, which would make named
        # tables (bronze.sessions, gold.dim_player, secure views, ...) invisible
        # to the next process.  Backing the catalog with Hive + an embedded Derby
        # metastore keeps it durable between CLI invocations, the way a managed
        # platform's catalog does.
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.javax.jdo.option.ConnectionURL": (
            "jdbc:derby:;databaseName=" + os.path.abspath(settings.metastore_dir) + ";create=true"
        ),
        "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.apache.derby.jdbc.EmbeddedDriver",
        "spark.driver.extraJavaOptions": (
            "-Dderby.system.home=" + os.path.abspath(os.path.dirname(settings.metastore_dir))
        ),
        "spark.driver.host": "127.0.0.1",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        # Structured Streaming: keep small local file sources responsive.
        "spark.sql.streaming.maxFilesPerTrigger": "32",
        # IMPORTANT: Spark >=3.0 prunes the parser's `_corrupt_record` column out
        # of the CSV/JSON read plan by default, so malformed records are dropped
        # instead of captured.  The bronze layer relies on `_corrupt_record` to
        # route unparseable rows into quarantine, so pruning is turned off.
        "spark.sql.csv.parser.columnPruning.enabled": "false",
        "spark.sql.json.parser.columnPruning.enabled": "false",
    }


def get_spark(app_name: str | None = None, **overrides: Any) -> SparkSession:
    """Return the process-wide :class:`SparkSession`, creating it if needed.

    ``overrides`` are applied as Spark conf entries (handy in tests, e.g.
    ``get_spark("t", **{"spark.sql.shuffle.partitions": "2"})``).
    """
    global _SESSION
    with _LOCK:
        if SparkSession.getActiveSession() is not None:
            _SESSION = SparkSession.getActiveSession()
            return _SESSION  # type: ignore[return-value]
        if _SESSION is not None:
            return _SESSION

        settings = get_settings()
        conf = delta_conf(settings, app_name=app_name)
        conf.update(overrides)

        builder = SparkSession.builder
        for key, value in conf.items():
            builder = builder.config(key, value)
        # `configure_spark_with_delta_pip` pins the delta-spark jar coordinates
        # so the OSS Delta runtime is present on the classpath.
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel(os.environ.get("DLG_SPARK_LOG_LEVEL", "ERROR"))
        _SESSION = session
        return session


def active_session() -> SparkSession | None:
    """Return the currently active session, if any (no side effects)."""
    return SparkSession.getActiveSession() or _SESSION


def stop_spark() -> None:
    """Stop the shared session (used between test suites)."""
    global _SESSION
    with _LOCK:
        session = SparkSession.getActiveSession() or _SESSION
        if session is not None:
            session.stop()
        _SESSION = None
