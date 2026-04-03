data "databricks_catalog" "loyalty" {
  name    = "loyalty"
}

resource "databricks_schema" "bronze" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "bronze"
  comment      = "Raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "silver"
  comment      = "Cleansed and deduplicated data"
}

resource "databricks_schema" "gold" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "gold"
  comment      = "Aggregated data for BI consumption"
}

resource "databricks_schema" "landing" {
  catalog_name = data.databricks_catalog.loyalty.name
  name         = "landing"
  comment      = "Raw landing zone"
}
resource "databricks_volume" "raw_data" {
  catalog_name = data.databricks_catalog.loyalty.name
  schema_name  = "landing"
  name         = "raw_data_tf"
  volume_type  = "MANAGED"
  comment      = "Raw landing zone for source files - managed by Terraform"
}