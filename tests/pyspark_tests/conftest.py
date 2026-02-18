"""Shared fixtures for PySpark integration tests."""

import os
import shutil

import pytest

_WAREHOUSE_DIR = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "spark-warehouse"
)


@pytest.fixture(scope="session", autouse=True)
def _cleanup_spark_warehouse():
    """Remove stale spark-warehouse before and after PySpark tests.

    Spark's DROP DATABASE only removes metastore entries, not the physical
    directory under spark-warehouse/.  If a previous test run was interrupted,
    the leftover directories cause LOCATION_ALREADY_EXISTS on the next run.
    """
    warehouse = os.path.normpath(_WAREHOUSE_DIR)
    if os.path.isdir(warehouse):
        shutil.rmtree(warehouse, ignore_errors=True)
    yield
    if os.path.isdir(warehouse):
        shutil.rmtree(warehouse, ignore_errors=True)
