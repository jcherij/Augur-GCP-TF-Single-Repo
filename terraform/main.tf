locals {
  name   = "augur-${var.environment}"
  pn     = data.google_project.this.number
  labels = {
    application         = "augur"
    environment         = var.environment
    owner               = "client-analytics"
    data-classification = "client-confidential"
  }

  apis = [
    "aiplatform.googleapis.com",
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudkms.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "managedkafka.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
  ]

  kafka_bootstrap   = "bootstrap.${google_managed_kafka_cluster.augur.cluster_id}.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092"
  landing_prefix    = "gs://${google_storage_bucket.landing.name}/client-activity"
  pipeline_root     = "gs://${google_storage_bucket.artifacts.name}/pipeline-root"
  pipeline_jobs_uri = "https://${var.region}-aiplatform.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/pipelineJobs"
  endpoint_resource = "projects/${var.project_id}/locations/${var.region}/endpoints/${google_vertex_ai_endpoint.augur.name}"
  api_image         = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.augur.repository_id}/augur-api:${var.inference_api_image_tag}"
}

data "google_project" "this" {
  project_id = var.project_id
}

data "google_storage_project_service_account" "gcs" {
  depends_on = [google_project_service.apis]
}

data "google_bigquery_default_service_account" "bq" {
  depends_on = [google_project_service.apis]
}

###############################################################################
# 1. Project foundation — APIs, service agents, CMEK
###############################################################################

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

resource "google_project_service_identity" "aiplatform" {
  provider   = google-beta
  service    = "aiplatform.googleapis.com"
  depends_on = [google_project_service.apis]
}

resource "google_kms_key_ring" "augur" {
  name       = local.name
  location   = var.region
  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "augur" {
  name            = "${local.name}-data"
  key_ring        = google_kms_key_ring.augur.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = "7776000s"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_kms_crypto_key_iam_member" "service_agents" {
  for_each = {
    vertex           = "serviceAccount:${google_project_service_identity.aiplatform.email}"
    pubsub           = "serviceAccount:service-${local.pn}@gcp-sa-pubsub.iam.gserviceaccount.com"
    managedkafka     = "serviceAccount:service-${local.pn}@gcp-sa-managedkafka.iam.gserviceaccount.com"
    storage          = "serviceAccount:${data.google_storage_project_service_account.gcs.email_address}"
    bigquery         = "serviceAccount:${data.google_bigquery_default_service_account.bq.email}"
    dataflow         = "serviceAccount:service-${local.pn}@dataflow-service-producer-prod.iam.gserviceaccount.com"
    compute          = "serviceAccount:service-${local.pn}@compute-system.iam.gserviceaccount.com"
    artifactregistry = "serviceAccount:service-${local.pn}@gcp-sa-artifactregistry.iam.gserviceaccount.com"
  }

  crypto_key_id = google_kms_crypto_key.augur.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = each.value
}

###############################################################################
# 2. Network — private VPC for Kafka and Dataflow workers
###############################################################################

resource "google_compute_network" "augur" {
  name                    = local.name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "augur" {
  name                     = "${local.name}-${var.region}"
  network                  = google_compute_network.augur.id
  region                   = var.region
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

resource "google_compute_firewall" "dataflow_internal" {
  name        = "${local.name}-dataflow-internal"
  network     = google_compute_network.augur.id
  description = "Dataflow worker-to-worker shuffle traffic"
  direction   = "INGRESS"
  priority    = 1000
  source_tags = ["dataflow"]
  target_tags = ["dataflow"]

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
}

resource "google_compute_firewall" "deny_all_ingress" {
  name          = "${local.name}-deny-all-ingress"
  network       = google_compute_network.augur.id
  description   = "Default deny for all other inbound traffic"
  direction     = "INGRESS"
  priority      = 65000
  source_ranges = ["0.0.0.0/0"]

  deny {
    protocol = "all"
  }
}

###############################################################################
# 3. Ingest — Managed Kafka -> Dataflow -> GCS landing bucket
###############################################################################

resource "google_managed_kafka_cluster" "augur" {
  provider   = google-beta
  cluster_id = local.name
  location   = var.region

  capacity_config {
    vcpu_count   = var.kafka_vcpu_count
    memory_bytes = var.kafka_memory_bytes
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = google_compute_subnetwork.augur.id
      }
    }
    kms_key = google_kms_crypto_key.augur.id
  }

  rebalance_config {
    mode = "AUTO_REBALANCE_ON_SCALE_UP"
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_managed_kafka_topic" "client_activity" {
  provider           = google-beta
  topic_id           = "client-activity"
  cluster            = google_managed_kafka_cluster.augur.cluster_id
  location           = var.region
  partition_count    = var.kafka_partitions
  replication_factor = 3

  configs = {
    "retention.ms"        = tostring(var.kafka_retention_ms)
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
}

resource "google_project_iam_member" "kafka_producers" {
  for_each = toset(var.kafka_producer_principals)
  project  = var.project_id
  role     = "roles/managedkafka.client"
  member   = each.value
}

resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-augur-landing"
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  encryption {
    default_kms_key_name = google_kms_crypto_key.augur.id
  }

  lifecycle_rule {
    condition {
      age = var.landing_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_storage_bucket" "dataflow" {
  name                        = "${var.project_id}-augur-dataflow"
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  encryption {
    default_kms_key_name = google_kms_crypto_key.augur.id
  }

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_service_account" "ingest" {
  account_id   = "augur-ingest"
  display_name = "Augur Kafka-to-GCS Dataflow workers"
}

resource "google_project_iam_member" "ingest" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/managedkafka.client",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "ingest_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "ingest_dataflow" {
  bucket = google_storage_bucket.dataflow.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_dataflow_flex_template_job" "kafka_to_gcs" {
  provider                = google-beta
  name                    = "${local.name}-kafka-to-gcs"
  region                  = var.region
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Kafka_to_Gcs_Flex"

  parameters = {
    readBootstrapServerAndTopic = "${local.kafka_bootstrap};${google_managed_kafka_topic.client_activity.topic_id}"
    kafkaReadAuthenticationMode = "APPLICATION_DEFAULT_CREDENTIALS"
    messageFormat               = "JSON"
    outputFileFormat            = "TEXT"
    outputDirectory             = "${local.landing_prefix}/"
    outputFilenamePrefix        = "events-"
    windowDuration              = var.landing_window
    enableCommitOffsets         = "true"
    consumerGroupId             = "augur-landing"
    kafkaReadOffset             = "earliest"
  }

  service_account_email   = google_service_account.ingest.email
  subnetwork              = google_compute_subnetwork.augur.self_link
  ip_configuration        = "WORKER_IP_PRIVATE"
  kms_key_name            = google_kms_crypto_key.augur.id
  temp_location           = "gs://${google_storage_bucket.dataflow.name}/temp"
  staging_location        = "gs://${google_storage_bucket.dataflow.name}/staging"
  machine_type            = "n2-standard-2"
  max_workers             = var.dataflow_max_workers
  enable_streaming_engine = true
  on_delete               = "drain"

  depends_on = [
    google_project_iam_member.ingest,
    google_storage_bucket_iam_member.ingest_landing,
    google_storage_bucket_iam_member.ingest_dataflow,
    google_compute_firewall.dataflow_internal,
  ]
}

###############################################################################
# 4. Analytical store — BigQuery over the landing bucket
###############################################################################

resource "google_bigquery_dataset" "augur" {
  dataset_id                 = "augur"
  friendly_name              = "Augur redemption risk"
  location                   = var.region
  delete_contents_on_destroy = false

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.augur.id
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_bigquery_table" "events" {
  dataset_id  = google_bigquery_dataset.augur.dataset_id
  table_id    = "client_activity_events"
  description = "External table over the raw client activity files that Dataflow lands from Kafka"

  external_data_configuration {
    source_format         = "NEWLINE_DELIMITED_JSON"
    autodetect            = false
    ignore_unknown_values = true
    max_bad_records       = 100
    source_uris           = ["${local.landing_prefix}/*"]

    schema = jsonencode([
      { name = "event_id", type = "STRING", mode = "REQUIRED" },
      { name = "client_id", type = "STRING", mode = "REQUIRED" },
      { name = "event_type", type = "STRING", mode = "REQUIRED" },
      { name = "amount_usd", type = "NUMERIC", mode = "NULLABLE" },
      { name = "aum_usd_after", type = "NUMERIC", mode = "NULLABLE" },
      { name = "source_system", type = "STRING", mode = "REQUIRED" },
      { name = "event_time", type = "TIMESTAMP", mode = "REQUIRED" },
    ])
  }
}

resource "google_bigquery_table" "training_examples" {
  dataset_id  = google_bigquery_dataset.augur.dataset_id
  table_id    = "client_training_examples"
  description = "Monthly labelled snapshots rebuilt by every training run"

  schema = jsonencode([
    { name = "snapshot_date", type = "DATE", mode = "REQUIRED" },
    { name = "client_id", type = "STRING", mode = "REQUIRED" },
    { name = "aum_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "portal_logins_30d", type = "INT64", mode = "NULLABLE" },
    { name = "advisor_contacts_90d", type = "INT64", mode = "NULLABLE" },
    { name = "complaints_12m", type = "INT64", mode = "NULLABLE" },
    { name = "deposits_90d_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "redemptions_90d_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "redeemed_90d", type = "BOOL", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "features_current" {
  dataset_id  = google_bigquery_dataset.augur.dataset_id
  table_id    = "client_features_current"
  description = "Today's feature vector per client, rebuilt by every training run and read by the inference API"

  schema = jsonencode([
    { name = "client_id", type = "STRING", mode = "REQUIRED" },
    { name = "as_of_date", type = "DATE", mode = "REQUIRED" },
    { name = "aum_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "portal_logins_30d", type = "INT64", mode = "NULLABLE" },
    { name = "advisor_contacts_90d", type = "INT64", mode = "NULLABLE" },
    { name = "complaints_12m", type = "INT64", mode = "NULLABLE" },
    { name = "deposits_90d_usd", type = "NUMERIC", mode = "NULLABLE" },
    { name = "redemptions_90d_usd", type = "NUMERIC", mode = "NULLABLE" },
  ])
}

###############################################################################
# 5. Training — daily Vertex AI pipeline
###############################################################################

resource "google_storage_bucket" "artifacts" {
  name                        = "${var.project_id}-augur-artifacts"
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  encryption {
    default_kms_key_name = google_kms_crypto_key.augur.id
  }

  lifecycle_rule {
    condition {
      age            = var.pipeline_root_retention_days
      matches_prefix = ["pipeline-root/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_storage_bucket_object" "training_pipeline" {
  bucket       = google_storage_bucket.artifacts.name
  name         = "pipelines/training_pipeline.json"
  content      = file("${path.module}/../pipelines/compiled/training_pipeline.json")
  content_type = "application/json"
}

resource "google_service_account" "pipeline" {
  account_id   = "augur-pipeline"
  display_name = "Augur training pipeline runner"
}

resource "google_project_iam_member" "pipeline" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/bigquery.jobUser",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline" {
  dataset_id = google_bigquery_dataset.augur.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_artifacts" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_service_account_iam_member" "pipeline_act_as_self" {
  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_scheduler_job" "training" {
  name             = "${local.name}-training"
  description      = "Daily Augur training from the previous day's landed events"
  region           = var.region
  schedule         = var.training_schedule
  time_zone        = var.schedule_time_zone
  attempt_deadline = "320s"

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = local.pipeline_jobs_uri
    headers     = { "Content-Type" = "application/json" }

    body = base64encode(jsonencode({
      displayName    = "${local.name}-training"
      templateUri    = "gs://${google_storage_bucket.artifacts.name}/${google_storage_bucket_object.training_pipeline.name}"
      serviceAccount = google_service_account.pipeline.email
      encryptionSpec = { kmsKeyName = google_kms_crypto_key.augur.id }
      labels         = local.labels
      runtimeConfig = {
        gcsOutputDirectory = local.pipeline_root
        parameterValues = {
          project_id          = var.project_id
          region              = var.region
          dataset_id          = google_bigquery_dataset.augur.dataset_id
          endpoint_id         = google_vertex_ai_endpoint.augur.name
          kms_key_name        = google_kms_crypto_key.augur.id
          model_updates_topic = google_pubsub_topic.model_updates.id
          min_auc             = var.min_auc
          lookback_months     = var.training_lookback_months
        }
      }
    }))

    oauth_token {
      service_account_email = google_service_account.pipeline.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_project_service.apis]
}

###############################################################################
# 6. Model serving — always-on Vertex AI endpoint
###############################################################################

resource "google_vertex_ai_endpoint" "augur" {
  name         = var.endpoint_id
  display_name = "augur-redemption-risk"
  description  = "Augur online redemption risk model, always on"
  location     = var.region
  region       = var.region

  encryption_spec {
    kms_key_name = google_kms_crypto_key.augur.id
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_project_iam_custom_role" "predictor" {
  role_id     = "augurPredictor"
  title       = "Augur predictor"
  description = "Online prediction against the Augur endpoint only"
  permissions = ["aiplatform.endpoints.predict"]
}

###############################################################################
# 7. Model update notification — pipeline -> Pub/Sub -> inference API
###############################################################################

resource "google_pubsub_topic" "model_updates" {
  name                       = "${local.name}-model-updates"
  kms_key_name               = google_kms_crypto_key.augur.id
  message_retention_duration = "604800s"

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_pubsub_topic_iam_member" "pipeline_publisher" {
  topic  = google_pubsub_topic.model_updates.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_service_account" "push" {
  account_id   = "augur-model-update-push"
  display_name = "Augur model update push identity"
}

resource "google_pubsub_subscription" "model_updates_to_api" {
  name                       = "${local.name}-model-updates-api"
  topic                      = google_pubsub_topic.model_updates.id
  ack_deadline_seconds       = 30
  message_retention_duration = "604800s"

  push_config {
    push_endpoint = "${google_cloud_run_v2_service.inference_api.uri}/internal/model-updated"

    oidc_token {
      service_account_email = google_service_account.push.email
      audience              = google_cloud_run_v2_service.inference_api.uri
    }
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

###############################################################################
# 8. Inference API — always-on Cloud Run service used by other apps
###############################################################################

resource "google_artifact_registry_repository" "augur" {
  repository_id = "augur"
  location      = var.region
  format        = "DOCKER"
  description   = "Augur container images"
  kms_key_name  = google_kms_crypto_key.augur.id

  docker_config {
    immutable_tags = true
  }

  depends_on = [google_kms_crypto_key_iam_member.service_agents]
}

resource "google_service_account" "inference_api" {
  account_id   = "augur-inference-api"
  display_name = "Augur inference API"
}

resource "google_project_iam_member" "inference_api" {
  for_each = {
    predictor = google_project_iam_custom_role.predictor.id
    bq_jobs   = "roles/bigquery.jobUser"
  }
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.inference_api.email}"
}

resource "google_bigquery_table_iam_member" "inference_api_features" {
  dataset_id = google_bigquery_dataset.augur.dataset_id
  table_id   = google_bigquery_table.features_current.table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.inference_api.email}"
}

resource "google_cloud_run_v2_service" "inference_api" {
  name                = "${local.name}-api"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = true

  template {
    service_account = google_service_account.inference_api.email

    scaling {
      min_instance_count = var.inference_api_min_instances
      max_instance_count = var.inference_api_max_instances
    }

    containers {
      image = local.api_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = false
      }

      env {
        name  = "VERTEX_ENDPOINT"
        value = local.endpoint_resource
      }
      env {
        name  = "FEATURES_TABLE"
        value = "${var.project_id}.${google_bigquery_dataset.augur.dataset_id}.${google_bigquery_table.features_current.table_id}"
      }
      env {
        name  = "PUSH_AUDIENCE_SA"
        value = google_service_account.push.email
      }

      startup_probe {
        http_get {
          path = "/healthz"
        }
      }

      liveness_probe {
        http_get {
          path = "/healthz"
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_service_iam_member" "callers" {
  for_each = merge(
    { for principal in var.inference_api_callers : principal => principal },
    { model_update_push = "serviceAccount:${google_service_account.push.email}" },
  )
  name     = google_cloud_run_v2_service.inference_api.name
  location = var.region
  role     = "roles/run.invoker"
  member   = each.value
}

###############################################################################
# 9. Monitoring
###############################################################################

resource "google_monitoring_alert_policy" "ingest_stalled" {
  display_name          = "${local.name} Kafka-to-GCS ingest stalled"
  combiner              = "OR"
  notification_channels = var.alert_notification_channels
  user_labels           = local.labels

  conditions {
    display_name = "Dataflow watermark older than 30 minutes"

    condition_threshold {
      filter          = "resource.type = \"dataflow_job\" AND resource.labels.job_name = \"${google_dataflow_flex_template_job.kafka_to_gcs.name}\" AND metric.type = \"dataflow.googleapis.com/job/data_watermark_age\""
      comparison      = "COMPARISON_GT"
      threshold_value = 1800
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "endpoint_errors" {
  display_name          = "${local.name} online prediction errors"
  combiner              = "OR"
  notification_channels = var.alert_notification_channels
  user_labels           = local.labels

  conditions {
    display_name = "Vertex endpoint error count above 5 in 5 minutes"

    condition_threshold {
      filter          = "resource.type = \"aiplatform.googleapis.com/Endpoint\" AND resource.labels.endpoint_id = \"${google_vertex_ai_endpoint.augur.name}\" AND metric.type = \"aiplatform.googleapis.com/prediction/online/error_count\""
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }
}
