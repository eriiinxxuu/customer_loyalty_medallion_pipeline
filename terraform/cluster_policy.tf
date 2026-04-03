resource "databricks_cluster_policy" "loyalty_job_cluster" {
  name = "loyalty-optimised-job-cluster"
  definition = jsonencode({
    "autotermination_minutes" : { "type" : "fixed", "value" : 20 },
    "autoscale.min_workers" : { "type" : "fixed", "value" : 2 },
    "autoscale.max_workers" : { "type" : "fixed", "value" : 8 }
  })
}