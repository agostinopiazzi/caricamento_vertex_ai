
REGION = "europe-west4a"
PROJECT_ID = "bper-ai-ew-test-380011"
ZONE="europe-west4-a"
BUCKET_NAME = "gs://bper-ai-ew-test-pipeline-integration"
PIPELINE_ROOT = f"{BUCKET_NAME}/pipeline_root/"
DATAPROC_CLUSTER="bper-ai-dataproc-cluster"

base_image="python:3.9",
output_component_file="load_data_from_bucket_component.yaml",
packages_to_install=["google-cloud-dataproc","google-cloud-storage"]

region="europe-west4"
cluster_name="bper-ai-dataproc-cluster"
uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"

main_python_file_uri = "gs://bper-ai-ew-test-pipeline-integration/load_data_job.py"


with open("template_job.txt", 'r', encoding='utf8') as source:
    lines = source.read()
    lines = lines.replace('@region@','\'europe-west4\'')
    with open("example.py", 'w') as target:
        target.write(lines)
