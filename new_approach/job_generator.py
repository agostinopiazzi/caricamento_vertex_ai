from kfp.v2.dsl import component
from kfp.components import create_component_from_func


def make_node(
            # job specific parameters
            pyfile_name: str,
            input_files_paths: list,
            output_file_path: str,
            base_image: str,
            output_component_file: str,
            packages_to_install: list
            ):

    def _job_func(
            # pipeline run parameters
            spark_properties_folder_path: str,
            spark_properties_file_name: str,
            helper_module_file_path: str,
            input_folder_path: str,
            output_folder_path: str,
            main_python_file_folder_path: str,
            cluster_name: str,
            region: str,
            uri_custom_image: str
    ):

        import re
        import google.cloud.dataproc_v1 as dataproc
        import google.cloud.storage as storage
        import os

        main_python_file_uri = os.path.join(main_python_file_folder_path, pyfile_name)
        input_files_full_paths = [os.path.join(input_folder_path, path) for path in input_files_paths]
        output_file_full_path = os.path.join(output_folder_path, output_file_path)
        spark_properties_file_path = os.path.join(spark_properties_folder_path, spark_properties_file_name)

        all_in_out_paths = input_files_full_paths + [output_file_full_path]
        all_in_out_paths.sort(key=lambda x: x.split('/')[-1])

        # Create the job client.
        job_client = dataproc.JobControllerClient(
            client_options = {"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
        )

        # Create the job config. 'main_jar_file_uri' can also be a
        # Google Cloud Storage URL.
        job = {
            'placement': {
                'cluster_name': cluster_name
            },
            'pyspark_job': {
                'main_python_file_uri': main_python_file_uri,
                'properties': {
                    "spark.kubernetes.container.image": uri_custom_image
                },
                'properties-file': spark_properties_file_path,
                'py-files': helper_module_file_path,
                'args': all_in_out_paths
            }
        }

        operation = job_client.submit_job_as_operation(
            request = {"project_id": project_id, "region": region, "job": job}
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

    _component_func = create_component_from_func(
        _job_func,
        output_component_file=output_component_file,
        packages_to_install=packages_to_install,
        base_image=base_image
    )

    return _component_func


