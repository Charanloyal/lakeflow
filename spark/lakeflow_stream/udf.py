"""Spark wrapper around lakeflow_core.decode, the single decoder implementation."""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType

from lakeflow_core.contracts import ContractRegistry
from lakeflow_core.decode import DECODED_SCHEMA, decode_change_event

_TYPES = {"string": StringType(), "int": IntegerType(), "bigint": LongType(), "array<string>": ArrayType(StringType())}
DECODED_STRUCT = StructType([StructField(name, _TYPES[kind], True) for name, kind in DECODED_SCHEMA])
_NAMES = tuple(name for name, _ in DECODED_SCHEMA)


def make_decoder(registry: ContractRegistry, pii_key: bytes):
    def decode(topic, key, value):
        out = decode_change_event(topic, key, value, registry, pii_key)
        return tuple(out[name] for name in _NAMES)

    # Non-deterministic so the optimizer never duplicates the UDF when the struct is expanded with `_d.*`.
    return F.udf(decode, DECODED_STRUCT).asNondeterministic()
