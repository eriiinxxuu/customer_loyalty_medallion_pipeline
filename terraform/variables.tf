# variables.tf

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address for pipeline failure notifications"
  type        = string
  default = "xuxinying77@outlook.com"
}

variable "data_engineer_group" {
  description = "Databricks group name for data engineers"
  type        = string
  default     = "data-engineers"
}

variable "analyst_group" {
  description = "Databricks group name for analysts (read-only Gold access)"
  type        = string
  default     = "analysts"
}
