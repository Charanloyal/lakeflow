"""Shared, dependency-free LakeFlow logic used by the Spark job, API, Airflow DAGs and benchmarks.

Must stay compatible with Python 3.10 (the Spark image interpreter) and use only the standard library.
"""

__version__ = "2.0.0"
