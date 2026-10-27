variable "project_id" {
  description = "Dedicated GCP project for Augur. No other application runs in this project."
  type        = string
}

variable "region" {
  description = "Region for every Augur resource and all client data"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "production"
}

# ── Network ─────────────────────────────────────

variable "subnet_cidr" {
  description = "CIDR for the Augur subnet (Dataflow workers and Kafka PSC endpoints)"
  type        = string
  default     = "10.40.0.0/24"
}

# ── Ingest ──────────────────────────────────────

variable "kafka_vcpu_count" {
  description = "vCPUs for the Managed Kafka cluster (minimum 3)"
  type        = number
  default     = 3
}

variable "kafka_memory_bytes" {
  description = "Memory for the Managed Kafka cluster (1-8 GiB per vCPU)"
  type        = number
  default     = 6442450944
}

variable "kafka_partitions" {
  description = "Partitions on the client-activity topic"
  type        = number
  default     = 6
}

variable "kafka_retention_ms" {
  description = "Retention on the client-activity topic in milliseconds"
  type        = number
  default     = 604800000
}

variable "kafka_producer_principals" {
  description = "IAM principals of the systems that publish client activity events"
  type        = list(string)
  default     = []
}

variable "dataflow_max_workers" {
  description = "Maximum Dataflow workers for the Kafka-to-GCS job"
  type        = number
  default     = 3
}

variable "landing_window" {
  description = "Window after which Dataflow writes a new file to the landing bucket"
  type        = string
  default     = "15m"
}

variable "landing_retention_days" {
  description = "Days raw events are kept in the landing bucket"
  type        = number
  default     = 400
}

# ── Training ────────────────────────────────────

variable "training_schedule" {
  description = "Cron schedule for the daily training pipeline"
  type        = string
  default     = "0 2 * * *"
}

variable "schedule_time_zone" {
  description = "Time zone for the training schedule"
  type        = string
  default     = "America/New_York"
}

variable "training_lookback_months" {
  description = "Months of labelled snapshots used for each training run"
  type        = number
  default     = 12
}

variable "min_auc" {
  description = "Minimum hold-out AUC before a new model is deployed"
  type        = number
  default     = 0.75
}

variable "pipeline_root_retention_days" {
  description = "Days pipeline run artifacts are kept"
  type        = number
  default     = 90
}

# ── Inference ───────────────────────────────────

variable "endpoint_id" {
  description = "Numeric ID for the Vertex AI endpoint (up to 10 digits, no leading zero)"
  type        = string
  default     = "2210070001"

  validation {
    condition     = can(regex("^[1-9][0-9]{0,9}$", var.endpoint_id))
    error_message = "endpoint_id must be numeric, up to 10 digits, with no leading zero."
  }
}

variable "inference_api_image_tag" {
  description = "Tag of the augur-api image in the Augur Artifact Registry repository"
  type        = string
  default     = "1.0.0"
}

variable "inference_api_min_instances" {
  description = "Minimum Cloud Run instances for the inference API (kept warm)"
  type        = number
  default     = 1
}

variable "inference_api_max_instances" {
  description = "Maximum Cloud Run instances for the inference API"
  type        = number
  default     = 5
}

variable "inference_api_callers" {
  description = "IAM principals allowed to call the inference API, e.g. the Advisor Workbench service account"
  type        = list(string)
  default     = []
}

# ── Monitoring ──────────────────────────────────

variable "alert_notification_channels" {
  description = "Cloud Monitoring notification channel IDs for Augur alerts"
  type        = list(string)
  default     = []
}
