"""Compile the Augur training pipeline to compiled/training_pipeline.json.

Terraform uploads the compiled spec to the artifacts bucket and Cloud
Scheduler submits it to Vertex AI Pipelines every 24 hours. Re-run this
after changing the pipeline, then run terraform apply.
"""

from pathlib import Path

from kfp import compiler

from training_pipeline import training_pipeline

out = Path(__file__).parent / "compiled"
out.mkdir(exist_ok=True)
compiler.Compiler().compile(training_pipeline, str(out / "training_pipeline.json"))
print(f"Compiled to {out / 'training_pipeline.json'}")
