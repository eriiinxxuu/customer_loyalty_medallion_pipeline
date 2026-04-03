data "databricks_catalog" "loyalty" {
  name    = "loyalty"
}

resource "databricks_schema" "bronze" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "bronze_tf"
  comment      = "Raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "silver_tf"
  comment      = "Cleansed and deduplicated data"
}

resource "databricks_schema" "gold" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "gold_tf"
  comment      = "Aggregated data for BI consumption"
}

resource "databricks_schema" "landing" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "landing_tf"
  comment      = "Raw landing zone"
}