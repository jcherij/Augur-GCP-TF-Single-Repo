# Augur — Requirements and Design

Owner: Client Analytics
Version: 2.1, October 2026
Cloud: GCP, us-central1

## 1. Purpose

Most of the client assets Meridian Capital loses leave through a small number of large redemptions. Relationship managers usually learn about them after the client has decided. Augur predicts which clients are likely to redeem more than half of their assets within the next 90 days, so that advisors can reach out first.

Augur is one system. Client activity events stream in through Kafka and are written to object storage. A model is retrained from that data every 24 hours. Each retrained model that passes its quality gate goes onto an inference endpoint that is always running, and the inference API used by other Meridian Capital applications is told to switch to it. Every piece of infrastructure involved is defined in this repository's Terraform.

## 2. System overview

```
 Client Portal ─┐
 CRM ───────────┼─► Managed Kafka (topic: client-activity) ── private subnet, CMEK
 Transfer Agent ┘          │
                           ▼
                Dataflow streaming job (Kafka → GCS), private worker IPs
                           │  new file every 15 minutes
                           ▼
                GCS landing bucket  gs://<project>-augur-landing/client-activity/
                           │
                           ▼  BigQuery external table
                BigQuery dataset `augur`
                           │
     Cloud Scheduler ──►  Vertex AI Pipelines, daily 02:00 New York
       (daily)             build datasets ─► train ─► AUC gate ─► register + deploy ─► notify
                                                                     │                  │
                                                                     ▼                  ▼
                                                Vertex AI endpoint (always on)   Pub/Sub model-updates
                                                                     ▲                  │ push (OIDC)
                                                                     │ predict          ▼
                                    Advisor Workbench ─────────► Cloud Run augur-api (always on)
                                                                     │
                                                                     └─ reads client_features_current
```

Events flow continuously from the source systems into Kafka. Dataflow consumes the topic and writes a new file of events to the landing bucket every 15 minutes. BigQuery reads those files in place through an external table, so the landed data is queryable as soon as it is written.

Once a day the training pipeline rebuilds the training data and today's client features from everything that has landed, then trains a new model. If the model is good enough, the pipeline deploys it to the always-on Vertex AI endpoint and publishes a message saying so. Pub/Sub pushes that message to the inference API, which switches to the new version. Advisor Workbench and any future consumer only ever talk to the inference API.

## 3. Requirements

### Ingest

**AUG-01.** Client activity reaches Augur only through the Managed Kafka topic `client-activity`.

**AUG-02.** Only the Client Portal, CRM and Transfer Agent feed service accounts may publish to Kafka.

**AUG-03.** The Kafka cluster has no public endpoint and is reachable only through Augur's private subnet.

**AUG-04.** The topic keeps messages for 7 days, uses replication factor 3 and requires 2 in-sync replicas. A broker loss must lose no events, and a consumer outage of up to a week must be recoverable.

**AUG-05.** Events are written to the landing bucket within 15 minutes of being published.

**AUG-06.** The Dataflow ingest workers use private IPs only and run as a dedicated ingest service account.

**AUG-07.** The ingest identity can create objects in the landing bucket but cannot read, overwrite or delete them.

**AUG-08.** If ingest falls more than 30 minutes behind, Client Analytics on-call is alerted.

### Storage and retention

**AUG-09.** Raw events are kept in the landing bucket for 400 days and then deleted automatically.

**AUG-10.** Dataflow temporary and staging files are deleted after 7 days.

**AUG-11.** Pipeline run artifacts are deleted after 90 days. Registered models are kept until Client Analytics retires them.

**AUG-12.** Training examples and current features are rebuilt in full by every training run, so neither holds data that is no longer in the landing bucket.

**AUG-13.** Model update messages are kept for 7 days and stored only in us-central1.

**AUG-14.** No bucket, dataset, topic or repository in Augur is publicly accessible.

### Training

**AUG-15.** A new model is trained every 24 hours at 02:00 New York time from all events landed up to that point.

**AUG-16.** The model is a binary classifier giving the probability that a client redeems more than 50% of their assets within 90 days.

**AUG-17.** Model inputs are limited to AUM, portal logins in the last 30 days, advisor contacts in the last 90 days, complaints in the last 12 months, deposits in the last 90 days and redemptions in the last 90 days.

**AUG-18.** Client names, contact details, account numbers and dates of birth are never published to Kafka, landed or used as model inputs. Clients are identified only by the pseudonymous `client_id` issued by the client master system.

**AUG-19.** Each run trains on the most recent 12 months of labelled monthly snapshots.

**AUG-20.** A model is deployed only if its AUC on a 20% hold-out set is at least 0.75. Otherwise the current model stays in service.

**AUG-21.** Training runs under a dedicated pipeline service account. Nobody trains or deploys a model by hand.

**AUG-22.** Every deployed model is a new version of one model in the Vertex AI Model Registry, so any score can be traced to the version that produced it.

### Serving and inference

**AUG-23.** The Vertex AI endpoint always has at least one replica serving and scales to three.

**AUG-24.** Other applications get scores only through the Augur inference API, never by calling the endpoint directly.

**AUG-25.** The inference API always has at least one warm instance and scales to five.

**AUG-26.** The inference API is reachable only from internal networks and only by explicitly granted callers. The first caller is Advisor Workbench.

**AUG-27.** The inference API's identity can only request predictions from the endpoint and read the current features table. It cannot deploy models or read training data or raw events.

**AUG-28.** More than five endpoint prediction errors in five minutes alerts Client Analytics on-call.

### Model update propagation

**AUG-29.** When a new model is deployed, the pipeline publishes its resource name, version and AUC to the `model-updates` topic.

**AUG-30.** The inference API is notified by authenticated push, using a dedicated push identity that can do nothing except invoke the API.

**AUG-31.** A notification the API fails to acknowledge is retried with backoff from 10 seconds to 10 minutes for up to 7 days.

**AUG-32.** Only the training pipeline may publish model updates.

### Security

**AUG-33.** All Augur data at rest is encrypted with one customer-managed key that Meridian Capital controls. This covers Kafka, landed files, Dataflow state, BigQuery, pipeline artifacts, models, the endpoint, Pub/Sub messages and container images. The key rotates every 90 days and is protected from deletion.

**AUG-34.** All Augur resources and data stay in us-central1.

**AUG-35.** Each workload runs as its own service account: ingest, pipeline, inference API and push. No workload uses the default compute service account.

**AUG-36.** The subnet denies all inbound traffic except Dataflow worker-to-worker traffic, and it records VPC flow logs.

**AUG-37.** Container images in Augur's registry cannot be overwritten once pushed.

### Platform

**AUG-38.** Augur runs in its own GCP project. No other application is deployed there.

**AUG-39.** All infrastructure is defined in this repository's Terraform. The training pipeline is defined and compiled from this repository.

**AUG-40.** Every resource that supports labels carries `application=augur`, `environment`, `owner=client-analytics` and `data-classification=client-confidential`.

## 4. Design

### Network

One custom-mode VPC, `augur-production`, has a single regional subnet, `10.40.0.0/24`. The subnet has Private Google Access, and VPC flow logs are on at 50% sampling.

The Managed Kafka cluster attaches to this subnet through Private Service Connect. Dataflow workers run in the same subnet with private IPs and reach Google APIs through Private Google Access. There is no NAT and no external IP anywhere in the design.

Two firewall rules apply. One allows TCP 12345–12346 between instances tagged `dataflow`, which Dataflow needs for shuffle. The other explicitly denies all other inbound traffic.

### Ingest

The Managed Kafka cluster has 3 vCPUs and 6 GiB of memory, uses CMEK, and rebalances partitions automatically when it scales up.

The `client-activity` topic has 6 partitions, replication factor 3, `min.insync.replicas=2` and 7-day retention. Producers authenticate with Google credentials. `roles/managedkafka.client` is granted only to the three producer service accounts and to the ingest workers.

The Dataflow job runs Google's `Kafka_to_Gcs_Flex` template. It reads with application default credentials under consumer group `augur-landing` and commits offsets. On a 15-minute window it writes text files of newline-delimited JSON under `client-activity/` in the landing bucket. Streaming Engine is on, workers are `n2-standard-2` scaling up to three, and the job drains rather than cancels on delete, so events in flight are not lost.

### Events

Each Kafka message is one JSON event describing one client action. `event_id` is unique per event and set by the producer. `client_id` is the pseudonymous ID from the client master system. `event_type` is one of `portal_login`, `advisor_contact`, `complaint`, `deposit` or `redemption`. Deposits and redemptions also carry `amount_usd` and `aum_usd_after`, the client's assets after the transaction. `source_system` names the producer (`client-portal`, `crm` or `transfer-agent`), and `event_time` is when the action happened, in UTC.

### Analytical store

The `augur` BigQuery dataset is the only dataset in the project. Its default encryption key is the Augur CMEK, and it holds three tables.

`client_activity_events` is an external table over every file under `landing/client-activity/`. Nothing writes to it directly. It is simply a SQL view of what Dataflow has landed, and only the training pipeline reads it.

`client_training_examples` holds one row per client per month for the last 12 complete months that have a full 90-day outcome window. Each row carries that month's features and a label saying whether the client went on to redeem more than half their assets in the following 90 days. The training pipeline rebuilds it on every run.

`client_features_current` holds one row per active client with features as of today. The training pipeline rebuilds it on every run, and it is the only table the inference API can read.

### Training pipeline

Cloud Scheduler calls the Vertex AI `pipelineJobs.create` API every day at 02:00 New York time, retrying up to three times. It uses an OAuth token for `augur-pipeline`. The request names the compiled pipeline spec in the artifacts bucket, the CMEK key and the pipeline root, and passes the dataset, endpoint, topic, AUC threshold and lookback period as parameters.

The pipeline runs five steps in order. First, `build_datasets` rebuilds the training examples and current features from the external events table. Second, `train_model` trains an XGBoost `binary:logistic` model on a stratified 80/20 split and records the hold-out AUC and the row count. Third, the AUC gate ends the run if AUC is below 0.75. Fourth, `register_and_deploy` uploads a new default version of `augur-redemption-risk` using Google's prebuilt XGBoost 1.7 serving container. It deploys that version on `n1-standard-2` with one to three replicas at 100% traffic, and only then undeploys the previous version. Fifth, `notify_model_update` publishes the new model's resource name, version, AUC and deployment time to `model-updates`.

### Model serving and inference API

The Vertex AI endpoint `augur-redemption-risk` is created by Terraform with CMEK. Because each deploy adds the new version before removing the old one, the endpoint is never empty once the first run has passed the gate.

`augur-api` is a Cloud Run service with internal-only ingress. It runs one to five instances with CPU always allocated, so the warm instance does not throttle between requests, and each instance has 1 vCPU and 512 MiB. It runs as `augur-inference-api`. Client Analytics builds the image and pushes it to the `augur` Artifact Registry repository in this project, which uses CMEK and immutable tags.

Advisor Workbench calls `GET /v1/clients/{client_id}/risk`. The API reads that client's row from `client_features_current`, sends the features to the endpoint, and returns the client ID, risk score, model version and the date the features were computed.

Pub/Sub calls `POST /internal/model-updated`. The API checks that the OIDC token was issued for the push identity, records the new model version and clears any cached features. Cloud Run uses `GET /healthz` for startup and liveness probes.

### Identities and access

`augur-ingest` is the Dataflow worker identity. It holds Dataflow Worker and Managed Kafka Client on the project, Object Creator on the landing bucket and Object Admin on the Dataflow bucket.

`augur-pipeline` runs the training pipeline and is the identity Cloud Scheduler uses to start it. It holds Vertex AI User and BigQuery Job User on the project. It also holds Data Editor on the `augur` dataset, Object Viewer on the landing bucket, Object Admin on the artifacts bucket, Publisher on the `model-updates` topic, and Service Account User on itself so that it can submit pipeline runs that run as itself.

`augur-inference-api` is the Cloud Run identity. It holds the custom role `augurPredictor`, which contains only `aiplatform.endpoints.predict`, plus BigQuery Job User on the project and Data Viewer on the `client_features_current` table alone.

`augur-model-update-push` is the identity Pub/Sub uses to sign push requests. Its only grant is Cloud Run Invoker on `augur-api`.

Outside Augur, the three producer service accounts hold Managed Kafka Client on the project, and the Advisor Workbench service account holds Cloud Run Invoker on `augur-api`.

### Encryption

One Cloud KMS key, `augur-production-data`, sits in key ring `augur-production` in us-central1. It rotates every 90 days and has `prevent_destroy` set in Terraform.

Eight service agents hold Encrypter/Decrypter on it, one for each service that stores Augur data:
- Managed Kafka, for cluster storage
- Cloud Storage, for the landing, Dataflow and artifacts buckets
- Dataflow and Compute Engine, for job state and worker disks
- BigQuery, for the dataset
- Vertex AI, for the endpoint, pipeline runs and models
- Pub/Sub, for the model update topic
- Artifact Registry, for the image repository

### Monitoring

Two alert policies notify Client Analytics on-call. The ingest alert fires when the Dataflow job's watermark is more than 30 minutes old for five minutes, which means events are not reaching the landing bucket. The prediction alert fires when the endpoint records more than five errors in a five-minute window.

## 5. Infrastructure inventory

The Terraform has 46 resource blocks.

The foundation is:
- `google_project_service.apis`, enabling 13 APIs
- `google_project_service_identity.aiplatform`, for the Vertex AI service agent
- the key ring and crypto key
- `google_kms_crypto_key_iam_member.service_agents`, granting the eight service agents access to the key

The network is the VPC, the subnet, and the two firewall rules.

Ingest covers:
- the Managed Kafka cluster and topic
- the producer IAM grants
- the landing and Dataflow buckets
- the ingest service account, with its project and bucket grants
- the Dataflow flex template job

The analytical store is the BigQuery dataset and its three tables.

Training covers:
- the artifacts bucket and the uploaded pipeline spec
- the pipeline service account, with its project, dataset, bucket and act-as grants
- the Cloud Scheduler job

Model serving is the Vertex AI endpoint and the custom predictor role.

Model updates are the Pub/Sub topic, the pipeline's publisher grant, the push service account and the push subscription.

The inference API covers:
- the Artifact Registry repository
- the inference service account, with its project and table grants
- the Cloud Run service and its invoker grants

Monitoring is the two alert policies.

## 6. Repository and deployment

```
augur/
├── docs/augur.md                  this document
├── pipelines/
│   ├── training_pipeline.py       KFP v2 pipeline
│   ├── compile.py
│   ├── requirements.txt
│   └── compiled/training_pipeline.json
└── terraform/
    ├── versions.tf  variables.tf  main.tf  outputs.tf
    └── terraform.tfvars.example
```

Deploy in this order:
1. Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and set the project, producer service accounts, inference API callers and alert channel.
2. Run `terraform apply -target=google_artifact_registry_repository.augur`, then push the `augur-api:1.0.0` image to the repository it creates.
3. Run `terraform apply` for everything else.
4. Run the training scheduler job once by hand. The endpoint has no model until the first run passes the AUC gate.

After changing the pipeline, run `python pipelines/compile.py` and then `terraform apply`. Terraform uploads the new compiled spec, and the next daily run uses it.
