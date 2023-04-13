from kfp.v2.google.client import AIPlatformClient

REGION = "europe-west4"
PROJECT_ID = "goreply-dataplex-quality-demo"
BUCKET_NAME = "gs://bper-pipeline-vertex"
PIPELINE_ROOT = f"{BUCKET_NAME}/pipeline_root/"
JSON_FILENAME = "pipeline.json"

api_client = AIPlatformClient(
    project_id=PROJECT_ID,
    region=REGION,
)

response = api_client.create_run_from_job_spec(
    job_spec_path=JSON_FILENAME,
    enable_caching=False,
    # this argument is necessary if you did not specify PIPELINE_ROOT as part of the pipeline definition.
    pipeline_root=PIPELINE_ROOT
)
