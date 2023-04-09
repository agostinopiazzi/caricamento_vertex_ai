from kfp.v2.dsl import component


@component(
    base_image="python:3.9",
    output_component_file="load_data_from_bucket.yaml",
    packages_to_install=["google-cloud-storage", "google-cloud-dataproc"]
)
def load_data_from_bucket():

    import re
    import google.cloud.dataproc_v1 as dataproc
    import google.cloud.storage as storage

    project_id = "bper-ai-ew-test-380011"
    region = "europe-west4"
    cluster_name = "bper-ai-dataproc-cluster"
    uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"
    properties_file_path = "tbd"
    main_python_file_uri = "gs://bper-ai-ew-test-pipeline-integration/load_data_job.py"
    extra_py_file_path = "tbd"
    input_files_paths = ["inp1", "inp2", ]
    output_file_path = "out"

    all_in_out_paths = input_files_paths + [output_file_path]
    all_in_out_paths.sort(key=lambda x: x.split('/')[-1])

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
            'main_python_file_uri': main_python_file_uri,
            'properties':{
                "spark.kubernetes.container.image": uri_custom_image
            },
            'properties-file': properties_file_path,
            'py-files': extra_py_file_path
            'args': all_in_out_paths
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


