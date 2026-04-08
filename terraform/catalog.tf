# ── Unity Catalog — existing catalog ─────────────────────────────
data "databricks_catalog" "loyalty" {
  name = "loyalty"
}


# ── Schemas ───────────────────────────────────────────────────────
resource "databricks_schema" "landing" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "landing"
  comment      = "Raw landing zone — source files dropped here by upstream systems"
}

resource "databricks_schema" "bronze" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "bronze"
  comment      = "Append-only raw ingestion layer — data preserved as-is"
}

resource "databricks_schema" "silver" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "silver"
  comment      = "Cleansed, deduplicated, schema-reconciled transactions"
}

resource "databricks_schema" "gold" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "gold"
  comment      = "Aggregated member summaries for BI consumption"
}

resource "databricks_schema" "dim" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "dim"
  comment      = "Dimension tables — store, product, date"
}

resource "databricks_schema" "meta" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "meta"
  comment      = "Pipeline metadata — job versions, watermarks"
}


# ── Landing Volume ────────────────────────────────────────────────
resource "databricks_volume" "landing_raw" {
  catalog_name = data.databricks_catalog.loyalty.name
  schema_name  = databricks_schema.landing.name
  name         = "raw_data"
  volume_type  = "MANAGED"
  comment      = "Landing zone for daily transaction files"

  depends_on = [databricks_schema.landing]
}