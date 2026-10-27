output "kafka_bootstrap" {
  description = "Bootstrap address producers use to publish to the client-activity topic"
  value       = local.kafka_bootstrap
}

output "landing_prefix" {
  description = "GCS prefix where Dataflow lands raw client activity events"
  value       = local.landing_prefix
}

output "endpoint_resource_name" {
  description = "Always-on Vertex AI endpoint serving the current model"
  value       = local.endpoint_resource
}

output "inference_api_url" {
  description = "Internal URL of the Augur inference API"
  value       = google_cloud_run_v2_service.inference_api.uri
}

output "model_updates_topic" {
  description = "Pub/Sub topic announcing each newly deployed model version"
  value       = google_pubsub_topic.model_updates.id
}

output "api_image_repository" {
  description = "Artifact Registry path for the augur-api image"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.augur.repository_id}"
}

output "kms_key" {
  description = "CMEK protecting all Augur data"
  value       = google_kms_crypto_key.augur.id
}
