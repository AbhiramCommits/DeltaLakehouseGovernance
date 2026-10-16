"""Central configuration for the Delta lakehouse.

All settings are read from the environment with a ``DLG_`` prefix, so the same
code runs against a local ``./lakehouse`` directory or an ``s3a://`` bucket
without a code change::

    DLG_LAKE_ROOT=s3a://my-bucket/lakehouse \
    DLG_CATALOG_FILE=governance/catalog.yml \
    dlg bronze

Defaults are deliberately local-first so the repository is runnable with zero
cloud setup.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Layers of the medallion architecture, in dependency order.
LAYERS = ("bronze", "silver", "gold", "ops", "secure")

#: Row-count multiplier applied when ``DLG_SCALE=small`` (used by CI).
SCALE_FACTORS = {"full": 1.0, "small": 0.02}


class Settings(BaseSettings):
    """Environment-driven lakehouse settings (``DLG_*`` variables)."""

    model_config = SettingsConfigDict(
        env_prefix="DLG_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- storage roots -------------------------------------------------
    lake_root: str = "./lakehouse"
    landing_root: str | None = None
    checkpoint_root: str | None = None

    # ---- governance ----------------------------------------------------
    catalog_file: str = "governance/catalog.yml"
    roles_file: str = "governance/roles.yml"

    # ---- results / docs ------------------------------------------------
    results_dir: str = "results"
    docs_dir: str = "docs"

    # ---- spark / metastore --------------------------------------------
    warehouse_dir: str = "spark-warehouse"
    metastore_dir: str = "spark-warehouse/metastore_db"

    # ---- spark ---------------------------------------------------------
    app_name: str = "DeltaLakehouseGovernance"
    shuffle_partitions: int = 8
    master: str = "local[*]"
    driver_memory: str = "4g"

    # ---- data generation ----------------------------------------------
    scale: str = "full"
    seed: int = 42
    n_players: int = 25_000
    n_sessions: int = 200_000
    n_match_events: int = 600_000
    n_purchases: int = 40_000
    #: Number of daily install cohorts (players install on days 0..n_days-1).
    n_days: int = 30
    #: Extra days of activity generated *after* the last install cohort.
    #: A D30 retention figure needs at least 31 observed days, so the default
    #: keeps a 30-day cohort window plus a 30-day observation tail, which lets
    #: every cohort mature for D1, D7 and D30.
    observation_days: int = 30
    #: Number of landing files each dataset is split across (micro-batch size).
    files_per_dataset: int = 4

    # ---- agent service -------------------------------------------------
    llm_base_url: str | None = None
    llm_api_key: str | None = None
    llm_model: str = "stub-rule-based"

    @field_validator("scale")
    @classmethod
    def _check_scale(cls, value: str) -> str:
        value = value.lower()
        if value not in SCALE_FACTORS:
            raise ValueError(f"scale must be one of {sorted(SCALE_FACTORS)}, got {value!r}")
        return value

    # ------------------------------------------------------------------
    # derived paths
    # ------------------------------------------------------------------
    @property
    def scale_factor(self) -> float:
        return SCALE_FACTORS[self.scale]

    @property
    def total_days(self) -> int:
        """Full observation window: install cohorts + the maturation tail."""
        return self.n_days + self.observation_days

    @property
    def is_remote_lake(self) -> bool:
        return self.lake_root.startswith(("s3a://", "s3://", "abfss://", "gs://"))

    @property
    def landing(self) -> str:
        """Landing zone for raw files.  Always local (or an explicit override)."""
        if self.landing_root:
            return self.landing_root
        return os.path.abspath("landing")

    @property
    def checkpoints(self) -> str:
        """Streaming checkpoints.  Kept local so object stores stay cheap."""
        if self.checkpoint_root:
            return self.checkpoint_root
        return os.path.abspath(os.path.join("_checkpoints"))

    @property
    def catalog_path(self) -> Path:
        return Path(self.catalog_file)

    @property
    def roles_path(self) -> Path:
        return Path(self.roles_file)

    @property
    def lake(self) -> str:
        """Absolute local lake root, or the raw URI for object-store roots.

        Local roots are absolutised because Spark resolves a relative
        ``CREATE TABLE ... LOCATION`` against ``spark.sql.warehouse.dir`` -- which
        would silently scatter table data inside the warehouse directory.
        """
        if self.is_remote_lake:
            return self.lake_root.rstrip("/")
        return os.path.abspath(self.lake_root)

    def lake_path(self, *parts: str) -> str:
        """Join ``parts`` onto :attr:`lake`, normalising separators."""
        root = self.lake.rstrip("/")
        tail = "/".join(p.strip("/") for p in parts if p)
        return f"{root}/{tail}" if tail else root

    def table_path(self, name: str) -> str:
        """Physical path of ``layer.table`` (e.g. ``bronze.sessions``)."""
        if "." not in name:
            raise ValueError(f"table name must be '<layer>.<table>', got {name!r}")
        layer, table = name.split(".", 1)
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return self.lake_path(layer, table)

    def stream_path(self, name: str) -> str:
        """Checkpoint location for a streaming query."""
        return os.path.join(self.checkpoints, name)

    def scaled(self, n: int) -> int:
        """Apply the scale factor to a target row count (never below 1)."""
        return max(1, int(round(n * self.scale_factor)))

    def ensure_dirs(self) -> None:
        for path in (self.landing, self.checkpoints, self.results_dir, self.docs_dir):
            if not path.startswith(("s3a://", "s3://")):
                print(f"ensuring directory {path}")
                Path(path).mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings`."""
    return Settings()


def reload_settings(**overrides: object) -> Settings:
    """Clear the cache and rebuild settings (used by tests)."""
    get_settings.cache_clear()
    settings = Settings(**overrides) if overrides else Settings()
    get_settings.cache_clear()
    return settings
