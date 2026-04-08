output "landing_volume_path" {
  description = "Full path to Landing Volume"
  value       = "/Volumes/${data.databricks_catalog.loyalty.name}/${databricks_schema.landing.name}/${databricks_volume.landing_raw.name}"
}

output "cluster_policy_id" {
  description = "Cluster policy ID"
  value       = databricks_cluster_policy.loyalty_pipeline.id
}

output "schemas_created" {
  description = "All Unity Catalog schemas provisioned by Terraform"
  value = [
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.landing.name}",
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.bronze.name}",
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.silver.name}",
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.gold.name}",
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.dim.name}",
    "${data.databricks_catalog.loyalty.name}.${databricks_schema.meta.name}",
  ]
}