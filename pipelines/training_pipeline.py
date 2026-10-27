"""Augur daily training pipeline (Vertex AI Pipelines / KFP v2).

1. build_datasets  - builds monthly labelled training snapshots and today's
                     feature vectors from the raw Kafka events that Dataflow
                     lands in GCS (read through a BigQuery external table).
2. train_model     - trains an XGBoost redemption-risk classifier.
3. auc gate        - stops here unless hold-out AUC >= min_auc.
4. register_and_deploy - new Model Registry version, deployed to the
                     always-on Vertex AI endpoint; previous version removed.
5. notify          - publishes to the model-updates Pub/Sub topic so the
                     Cloud Run inference API switches to the new version.
"""

from typing import NamedTuple

from kfp import dsl
from kfp.dsl import Input, Metrics, Model, Output

FEATURE_COLUMNS = [
    "aum_usd",
    "portal_logins_30d",
    "advisor_contacts_90d",
    "complaints_12m",
    "deposits_90d_usd",
    "redemptions_90d_usd",
]


@dsl.component(base_image="python:3.11-slim", packages_to_install=["google-cloud-bigquery==3.25.0"])
def build_datasets(project_id: str, dataset_id: str, lookback_months: int) -> int:
    from google.cloud import bigquery

    events = f"{project_id}.{dataset_id}.client_activity_events"

    def features_sql(snapshots_sql: str) -> str:
        return f"""
        WITH snapshots AS ({snapshots_sql}),
        events AS (
          SELECT client_id, event_type, amount_usd, aum_usd_after, event_time
          FROM `{events}`
        )
        SELECT
          s.snapshot_date,
          e.client_id,
          ARRAY_AGG(e.aum_usd_after IGNORE NULLS ORDER BY e.event_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS aum_usd,
          COUNTIF(e.event_type = 'portal_login'
                  AND e.event_time >= TIMESTAMP(DATE_SUB(s.snapshot_date, INTERVAL 30 DAY))) AS portal_logins_30d,
          COUNTIF(e.event_type = 'advisor_contact'
                  AND e.event_time >= TIMESTAMP(DATE_SUB(s.snapshot_date, INTERVAL 90 DAY))) AS advisor_contacts_90d,
          COUNTIF(e.event_type = 'complaint') AS complaints_12m,
          SUM(IF(e.event_type = 'deposit'
                 AND e.event_time >= TIMESTAMP(DATE_SUB(s.snapshot_date, INTERVAL 90 DAY)), e.amount_usd, 0)) AS deposits_90d_usd,
          SUM(IF(e.event_type = 'redemption'
                 AND e.event_time >= TIMESTAMP(DATE_SUB(s.snapshot_date, INTERVAL 90 DAY)), e.amount_usd, 0)) AS redemptions_90d_usd
        FROM snapshots s
        JOIN events e
          ON e.event_time < TIMESTAMP(s.snapshot_date)
         AND e.event_time >= TIMESTAMP(DATE_SUB(s.snapshot_date, INTERVAL 365 DAY))
        GROUP BY s.snapshot_date, e.client_id
        """

    training_snapshots = f"""
        SELECT snapshot_date FROM UNNEST(GENERATE_DATE_ARRAY(
          DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL {lookback_months} MONTH),
          DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH),
          INTERVAL 1 MONTH)) AS snapshot_date
    """

    script = f"""
    TRUNCATE TABLE `{project_id}.{dataset_id}.client_training_examples`;
    INSERT INTO `{project_id}.{dataset_id}.client_training_examples`
    WITH f AS ({features_sql(training_snapshots)}),
    future AS (
      SELECT f.snapshot_date, f.client_id,
             SUM(IF(e.event_type = 'redemption', e.amount_usd, 0)) AS redeemed_next_90d_usd
      FROM f
      LEFT JOIN `{events}` e
        ON e.client_id = f.client_id
       AND e.event_time >= TIMESTAMP(f.snapshot_date)
       AND e.event_time < TIMESTAMP(DATE_ADD(f.snapshot_date, INTERVAL 90 DAY))
      GROUP BY 1, 2
    )
    SELECT f.*, COALESCE(future.redeemed_next_90d_usd, 0) > 0.5 * COALESCE(f.aum_usd, 0) AS redeemed_90d
    FROM f JOIN future USING (snapshot_date, client_id)
    WHERE f.aum_usd > 0;

    TRUNCATE TABLE `{project_id}.{dataset_id}.client_features_current`;
    INSERT INTO `{project_id}.{dataset_id}.client_features_current`
    SELECT client_id, snapshot_date AS as_of_date, aum_usd, portal_logins_30d, advisor_contacts_90d,
           complaints_12m, deposits_90d_usd, redemptions_90d_usd
    FROM ({features_sql("SELECT CURRENT_DATE() AS snapshot_date")})
    WHERE aum_usd > 0;
    """

    client = bigquery.Client(project=project_id)
    client.query(script).result()
    rows = client.query(
        f"SELECT COUNT(*) AS n FROM `{project_id}.{dataset_id}.client_training_examples`"
    ).result()
    return list(rows)[0]["n"]


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery[pandas]==3.25.0",
        "db-dtypes==1.2.0",
        "pandas==2.2.2",
        "scikit-learn==1.3.2",
        "xgboost==1.7.6",
    ],
)
def train_model(
    project_id: str,
    dataset_id: str,
    feature_columns: list,
    model: Output[Model],
    metrics: Output[Metrics],
) -> NamedTuple("TrainOutputs", [("auc", float)]):
    import os
    from collections import namedtuple

    import xgboost as xgb
    from google.cloud import bigquery
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    df = bigquery.Client(project=project_id).query(
        f"SELECT {', '.join(feature_columns)}, redeemed_90d "
        f"FROM `{project_id}.{dataset_id}.client_training_examples`"
    ).to_dataframe()

    x = df[feature_columns].astype(float).fillna(0).values
    y = df["redeemed_90d"].astype(int).values
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, stratify=y, random_state=42
    )

    booster = xgb.train(
        {"objective": "binary:logistic", "eval_metric": "auc", "max_depth": 4, "eta": 0.1},
        xgb.DMatrix(x_train, label=y_train),
        num_boost_round=200,
    )
    auc = float(roc_auc_score(y_test, booster.predict(xgb.DMatrix(x_test))))

    os.makedirs(model.path, exist_ok=True)
    booster.save_model(os.path.join(model.path, "model.bst"))
    model.metadata["feature_columns"] = feature_columns
    metrics.log_metric("auc", auc)
    metrics.log_metric("training_rows", len(df))

    return namedtuple("TrainOutputs", ["auc"])(auc)


@dsl.component(base_image="python:3.11-slim", packages_to_install=["google-cloud-aiplatform==1.71.1"])
def register_and_deploy(
    project_id: str,
    region: str,
    endpoint_id: str,
    kms_key_name: str,
    model_display_name: str,
    model: Input[Model],
) -> NamedTuple("DeployOutputs", [("model_resource", str), ("model_version", str)]):
    from collections import namedtuple

    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region, encryption_spec_key_name=kms_key_name)

    existing = aiplatform.Model.list(filter=f'display_name="{model_display_name}"')
    uploaded = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=model.uri,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
        parent_model=existing[0].resource_name if existing else None,
        is_default_version=True,
        labels={"application": "augur"},
    )

    endpoint = aiplatform.Endpoint(endpoint_name=endpoint_id)
    previous = [deployed.id for deployed in endpoint.list_models()]
    endpoint.deploy(
        model=uploaded,
        deployed_model_display_name=f"{model_display_name}-v{uploaded.version_id}",
        machine_type="n1-standard-2",
        min_replica_count=1,
        max_replica_count=3,
        traffic_percentage=100,
    )
    for deployed_model_id in previous:
        endpoint.undeploy(deployed_model_id=deployed_model_id)

    return namedtuple("DeployOutputs", ["model_resource", "model_version"])(
        uploaded.resource_name, str(uploaded.version_id)
    )


@dsl.component(base_image="python:3.11-slim", packages_to_install=["google-cloud-pubsub==2.23.1"])
def notify_model_update(topic: str, model_resource: str, model_version: str, auc: float):
    import json
    from datetime import datetime, timezone

    from google.cloud import pubsub_v1

    message = {
        "model_resource": model_resource,
        "model_version": model_version,
        "auc": auc,
        "deployed_at": datetime.now(timezone.utc).isoformat(),
    }
    pubsub_v1.PublisherClient().publish(topic, json.dumps(message).encode()).result()


@dsl.pipeline(name="augur-training")
def training_pipeline(
    project_id: str,
    region: str,
    dataset_id: str,
    endpoint_id: str,
    kms_key_name: str,
    model_updates_topic: str,
    model_display_name: str = "augur-redemption-risk",
    min_auc: float = 0.75,
    lookback_months: int = 12,
):
    build = build_datasets(project_id=project_id, dataset_id=dataset_id, lookback_months=lookback_months)

    train = train_model(
        project_id=project_id, dataset_id=dataset_id, feature_columns=FEATURE_COLUMNS
    ).after(build)
    train.set_cpu_limit("4").set_memory_limit("16G")

    with dsl.If(train.outputs["auc"] >= min_auc, name="auc-gate"):
        deploy = register_and_deploy(
            project_id=project_id,
            region=region,
            endpoint_id=endpoint_id,
            kms_key_name=kms_key_name,
            model_display_name=model_display_name,
            model=train.outputs["model"],
        )
        notify_model_update(
            topic=model_updates_topic,
            model_resource=deploy.outputs["model_resource"],
            model_version=deploy.outputs["model_version"],
            auc=train.outputs["auc"],
        )
