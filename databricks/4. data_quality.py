# dq_silver_to_gold.py
# Workflow Task 2.5 — runs AFTER Silver MERGE, BEFORE Gold aggregation.
# Great Expectations v1.15.2 API.
# %pip install great-expectations==1.15.2

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectTableRowCountToBeBetween,
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_TABLE = "loyalty.silver.transactions"

# ── Load Silver as Pandas sample for GE ──────────────────────────
# GE 1.x works with Pandas DataFrames natively.
# We sample 500K rows instead of full 50M — sufficient for DQ checks
# on distribution-based expectations. Business key uniqueness is
# validated separately via Spark to avoid sampling bias.
logger.info(f"Loading Silver sample for GE validation")

silver_spark = spark.table(SILVER_TABLE)
total_rows   = silver_spark.count()
logger.info(f"Total Silver rows: {total_rows:,}")

# Sample for GE — 1% or 500K rows, whichever is smaller
sample_fraction = min(0.01, 500_000 / total_rows)
silver_pd = silver_spark.sample(fraction=sample_fraction, seed=42).toPandas()
logger.info(f"Sample size for GE: {len(silver_pd):,} rows")


# ── GE v1.15.2 setup ─────────────────────────────────────────────
context = gx.get_context()

data_source  = context.data_sources.add_pandas(name="silver_pandas")
data_asset   = data_source.add_dataframe_asset(name="silver_sample")
batch_def    = data_asset.add_batch_definition_whole_dataframe("silver_batch")
batch        = batch_def.get_batch(batch_parameters={"dataframe": silver_pd})

suite = context.suites.add(
    gx.ExpectationSuite(name="silver_to_gold_suite")
)


# ── Critical expectations ─────────────────────────────────────────
suite.add_expectation(
    ExpectColumnValuesToNotBeNull(column="transaction_id")
)
suite.add_expectation(
    ExpectColumnValuesToNotBeNull(column="member_id")
)
suite.add_expectation(
    ExpectColumnValuesToNotBeNull(column="transaction_date")
)
suite.add_expectation(
    ExpectColumnValuesToBeBetween(
        column="amount",
        min_value=0.01,
        mostly=1.0
    )
)
suite.add_expectation(
    ExpectColumnValuesToBeInSet(
        column="channel",
        value_set=["POS", "mobile_app", "partner_api"],
        mostly=1.0
    )
)
suite.add_expectation(
    ExpectColumnValuesToBeInSet(
        column="tier",
        value_set=["bronze", "silver", "gold", "platinum"],
        mostly=1.0
    )
)


# ── Non-critical expectations ─────────────────────────────────────
suite.add_expectation(
    ExpectColumnValuesToBeBetween(
        column="amount",
        min_value=5.0,
        max_value=2000.0,
        mostly=0.99
    )
)
suite.add_expectation(
    ExpectTableRowCountToBeBetween(
        min_value=100,
        max_value=None
    )
)


# ── Run validation ────────────────────────────────────────────────
validation_def = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="silver_validation",
        data=batch_def,
        suite=suite,
    )
)

results = validation_def.run(
    batch_parameters={"dataframe": silver_pd}
)

logger.info(f"GE validation success: {results.success}")


# ── Spark-level critical checks (no sampling) ─────────────────────
# transaction_id uniqueness must be checked on full dataset —
# sampling cannot reliably detect duplicates.
logger.info("Running full-scan uniqueness check on transaction_id")

dup_count = silver_spark \
    .groupBy("transaction_id") \
    .count() \
    .filter("count > 1") \
    .count()

if dup_count > 0:
    raise ValueError(
        f"CRITICAL: {dup_count:,} duplicate transaction_ids found in Silver. "
        f"Gold will not be computed."
    )

logger.info("Uniqueness check passed — no duplicate transaction_ids")


# ── Handle GE failures ────────────────────────────────────────────
critical_types = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_in_set",
}

failed = [r for r in results.results if not r.success]

if failed:
    critical_failures = [
        r for r in failed
        if r.expectation_config.type in critical_types
        and r.expectation_config.kwargs.get("mostly", 1.0) == 1.0
    ]

    for r in failed:
        col_name = r.expectation_config.kwargs.get("column", "table-level")
        observed = r.result.get("unexpected_percent", "N/A")
        logger.warning(
            f"FAILED: {r.expectation_config.type} on '{col_name}' "
            f"— unexpected %: {observed}"
        )

    if critical_failures:
        raise ValueError(
            f"{len(critical_failures)} critical GE check(s) failed. "
            f"Gold layer will not be computed."
        )
    else:
        logger.warning(
            f"{len(failed)} non-critical check(s) failed — proceeding to Gold"
        )
else:
    logger.info("All GE checks passed — proceeding to Gold")