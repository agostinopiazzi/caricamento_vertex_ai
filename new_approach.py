from kfp.v2.dsl import component


def make_node(project_id, region, cluster_name, uri_custom_image, main_python_file_uri,
              spark_properties_file_path, helper_module_file_path, input_files_paths, output_file_path,
              packages_to_install, base_image, output_component_file, function_name, pyfile_name,
              ):

    @component(
        base_image=base_image,
        output_component_file=output_component_file,
        packages_to_install=packages_to_install
    )
    def _func(
        input_folder_path,
        output_folder_path,
        main_python_file_folder_path,
        spark_properties_folder_path,
        helper_module_file_path
        ):

        import re
        import google.cloud.dataproc_v1 as dataproc
        import google.cloud.storage as storage
        import os


        main_python_file_uri = os.path.join(main_python_file_folder_path, pyfile_name)
        input_files_paths = [os.path.join(input_folder_path, path) for path in input_files_paths]
        output_file_path = os.path.join(output_folder_path, output_file_path)
        spark_properties_file_path = os.path.join(spark_properties_folder_path, spark_properties_file_name)

        all_in_out_paths = input_files_paths + [output_file_path]
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
                'py-files': helper_module_file_path
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

    return _func


from kfp import dsl

def make_pipeline(name, transform, dataset, graph):

    free_resources = set()
    ...



    @dsl.pipeline(
        name=name,
    )
    def _pipeline():

        while free_resources:
            current_resource = free_resources.pop()
            components[current_resource] = make_node(...)
            for descendent in descendents[current_resource]:
                ancestors[descendent].remove(current_resource)
                if not ancestors[descendent]:
                    free_resources.add(descendent)

            for ancestor in ancestors_orig[current_resource]:
                components[current_resource].after(components[ancestor])


    return _pipeline

