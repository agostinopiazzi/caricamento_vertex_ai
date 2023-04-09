
# !pip3 install google-cloud-aiplatform==1.0.0 --upgrade
# !pip3 install kfp google-cloud-pipeline-components==1.0.8

REGION='europe-west4'
PROJECT_ID=@project_id@
ZONE="europe-west4-a"
BUCKET_NAME = "gs://bper-ai-ew-test-pipeline-integration"
PIPELINE_ROOT = f"{BUCKET_NAME}/pipeline_root/"
DATAPROC_CLUSTER="bper-ai-dataproc-cluster"


project_id=PROJECT_ID
region=REGION
cluster_name=DATAPROC_CLUSTER

from typing import NamedTuple
import kfp
from kfp import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,
                        OutputPath, ClassificationMetrics, Metrics, component)
from kfp.v2.google.client import AIPlatformClient
from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component


@component(
    base_image="python:3.9", 
    output_component_file="load_data_from_bucket_component.yaml", 
    packages_to_install=["google-cloud-dataproc","google-cloud-storage"]
)
def load_data_from_bucket():
    
    import re

    import google.cloud.dataproc_v1 as dataproc
    import google.cloud.storage as storage
    
    import json
    
    project_id="bper-ai-ew-test-380011"
    region="europe-west4"
    cluster_name="bper-ai-dataproc-cluster"
    uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"
    
    
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': "gs://bper-ai-ew-test-pipeline-integration/load_data_job.py",
            'properties':{
                "spark.kubernetes.container.image": uri_custom_image
            },
        }
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")


# +
#cmp1 = kfp.components.load_component_from_file("dataproc-cluster-component.yaml")

# +
@dsl.pipeline(
    name="prediction-bper",
    
)
def test_bper():
    task1 = load_data_from_bucket()


# -

compiler.Compiler().compile(
    pipeline_func=test_bper, package_path="bper_test_dataproc_job.json"
)

api_client = AIPlatformClient(
    project_id=PROJECT_ID,
    region=REGION,
)
response = api_client.create_run_from_job_spec(
    job_spec_path="bper_test_dataproc_job.json",
    enable_caching=False,
    pipeline_root=PIPELINE_ROOT  # this argument is necessary if you did not specify PIPELINE_ROOT as part of the pipeline definition.
)


