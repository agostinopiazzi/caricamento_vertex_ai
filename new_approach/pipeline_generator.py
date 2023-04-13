from kfp import dsl
from job_generator import make_node
import os


def make_pipeline(
        # pipeline specific arguments
        pipeline_name,
        area_dict,
        yaml_folder_path
):
    @dsl.pipeline(
        name=pipeline_name,
    )
    def _pipeline_func(
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

        # iterating on all the jobs in the area
        for job_key, job_dict in area_dict.items():

            job_name = job_dict['name']
            pyfile_name = job_name + '.py'
            input_files_paths = job_dict['inputs']
            output_file_path = job_dict['output'][0]
            base_image = "python:3.9"
            output_component_file = os.path.join(yaml_folder_path, job_name + ".yaml")
            packages_to_install = ["google-cloud-storage", "google-cloud-dataproc"]

            job_task = make_node(
                pyfile_name,
                input_files_paths,
                output_file_path,
                base_image,
                output_component_file,
                packages_to_install
            )(
                spark_properties_folder_path,
                spark_properties_file_name,
                helper_module_file_path,
                input_folder_path,
                output_folder_path,
                main_python_file_folder_path,
                cluster_name,
                region,
                uri_custom_image
            )
            job_task.set_display_name(job_name)
            job_dict['object'].append(job_task)

        for job_name, job_dict in area_dict.items():
            for ancestor in job_dict['ancestors']:
                if ancestor in area_dict:
                    job_dict['object'][0].after(area_dict[ancestor]['object'][0])

    return _pipeline_func

