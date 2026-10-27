# Augur

Client redemption risk prediction for Meridian Capital on GCP. Client activity streams through Managed Kafka into Cloud Storage, a Vertex AI pipeline retrains the model every 24 hours, and each new model is deployed to an always-on Vertex AI endpoint behind the Cloud Run inference API.

- `docs/augur.md` — requirements and design
- `terraform/` — all infrastructure
- `pipelines/` — the daily training pipeline and its compiled spec
