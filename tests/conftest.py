"""Shared pytest fixtures.

Everything runs against a **temporary lake root** so the suite never touches the
developer's `./lakehouse`.  A session-scoped fixture generates a small (2%)
dataset and builds the whole medallion once; individual tests then assert against
real Delta tables instead of mocks.

Fixture order matters:

``dlg_env``         -> points every DLG_* setting at the temp directory
``spark``           -> one SparkSession for the whole session
``lakehouse``       -> landing zone + bronze + silver + gold + governed views
``tmp_table``       -> a factory for throwaway Delta tables at temp paths
"""

from __future__ import annotations

import contextlib
import os
import shutil
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def dlg_env(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Point the lakehouse at a temporary directory (before Spark starts)."""
    root = tmp_path_factory.mktemp("lakehouse")
    env = {
        "DLG_LAKE_ROOT": str(root / "lakehouse"),
        "DLG_LANDING_ROOT": str(root / "landing"),
        "DLG_CHECKPOINT_ROOT": str(root / "_checkpoints"),
        "DLG_WAREHOUSE_DIR": str(root / "spark-warehouse"),
        "DLG_METASTORE_DIR": str(root / "spark-warehouse" / "metastore_db"),
        "DLG_RESULTS_DIR": str(root / "results"),
        "DLG_DOCS_DIR": str(root / "docs"),
        "DLG_CATALOG_FILE": str(REPO_ROOT / "governance" / "catalog.yml"),
        "DLG_ROLES_FILE": str(REPO_ROOT / "governance" / "roles.yml"),
        "DLG_SCALE": "small",
        "DLG_SHUFFLE_PARTITIONS": "4",
        "DLG_DRIVER_MEMORY": "2g",
    }
    os.environ.update(env)

    from dlg.config import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    settings.ensure_dirs()
    yield env
    print(f"\n[tests] temporary lake root: {settings.lake_root}")


@pytest.fixture(scope="session")
def spark(dlg_env):
    from dlg.spark import get_spark, stop_spark

    session = get_spark("dlg-tests")
    yield session
    stop_spark()


@pytest.fixture(scope="session")
def settings(dlg_env):
    from dlg.config import get_settings

    return get_settings()


@pytest.fixture(scope="session")
def lakehouse(spark, settings) -> dict:
    """Generate the small dataset and build bronze -> silver -> gold -> views."""
    from dlg.generate_data import Generator
    from dlg.governance.policies import apply_policies
    from dlg.pipelines.bronze import run_bronze
    from dlg.pipelines.gold import build_all
    from dlg.pipelines.silver import run_silver

    Generator(settings).run(clean=True)
    bronze = run_bronze(spark)
    silver = run_silver(spark)
    gold = build_all(spark)
    policies = apply_policies(spark, settings)

    return {
        "bronze": {k: v.as_dict() for k, v in bronze.items()},
        "silver": {k: v.as_dict() for k, v in silver.items()},
        "gold": gold.as_dict(),
        "views": policies.views_created,
    }


@pytest.fixture(scope="session")
def star(lakehouse):
    """A fixture that simply guarantees the lakehouse exists before a test runs."""
    return lakehouse


@pytest.fixture
def tmp_table(spark, tmp_path):
    """Factory: create a throwaway Delta table at a temp path.

    Returns ``(identifier, path)`` where ``identifier`` is the backticked
    ``delta.`/path` form usable in Spark SQL.
    """
    created: list[str] = []

    def _make(name: str, schema, partition_by: list[str] | None = None):
        from dlg.lakehouse import ddl_for

        path = str(tmp_path / name)
        identifier = f"delta.`{path}`"
        spark.sql(ddl_for(identifier, schema, path, partition_by=partition_by))
        created.append(identifier)
        return identifier, path

    yield _make
    for identifier in created:
        with contextlib.suppress(Exception):  # best-effort cleanup
            spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture
def reference_year(spark) -> int:
    from dlg.governance.policies import reference_year as ref

    return ref(spark)
