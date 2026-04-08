# ── Cluster policy ────────────────────────────────────────────────
# Enforces cost controls and standard configuration across all
# pipeline clusters — prevents ad-hoc oversized cluster creation.
resource "databricks_cluster_policy" "loyalty_pipeline" {
  name = "loyalty-pipeline-policy"

  definition = jsonencode({
    "spark_version" = {
      "type"  = "fixed"
      "value" = "14.3.x-scala2.12"
    }
    "node_type_id" = {
      "type"         = "allowlist"
      "values"       = ["i3.xlarge", "i3.2xlarge"]
      "defaultValue" = "i3.xlarge"
    }
    "autoscale.min_workers" = {
      "type"  = "fixed"
      "value" = 2
    }
    "autoscale.max_workers" = {
      "type"         = "range"
      "minValue"     = 2
      "maxValue"     = 8
      "defaultValue" = 4
    }
    "autotermination_minutes" = {
      "type"  = "fixed"
      "value" = 30
    }
    "spark_conf.spark.databricks.delta.preview.enabled" = {
      "type"  = "fixed"
      "value" = "true"
    }
  })
}