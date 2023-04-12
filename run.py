
from kfp.v2.google.client import AIPlatformClient

REGION="europe-west4"
PROJECT_ID="goreply-dataplex-quality-demo"
PIPELINE_ROOT =f"{BUCKET_NAME}/pipeline_root/"

api_client = AIPlatformClient(
    project_id=PROJECT_ID,
    region=REGION,
)
response = api_client.create_run_from_job_spec(
   job_spec_path="bper_test_dataproc_job.json",
 enable_caching=False,
    pipeline_root=PIPELINE_ROOT  # this argument is necessary if you did not specify PIPELINE_ROOT as part of the pipeline definition.
)