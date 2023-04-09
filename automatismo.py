

project_id = "bper-ai-ew-test-380011"
region = "europe-west4"
cluster_name = "bper-ai-dataproc-cluster"
uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"
properties_file_path = "tbd"
main_python_file_uri = "gs://bper-ai-ew-test-pipeline-integration/load_data_job.py"
extra_py_file_path ="tbd"
input_files_paths = ["inp1", "inp2"]
output_file_path = "out"

base_image = "python:3.9"
output_component_file = "load_data_from_bucket.yaml"
packages_to_install = "[\"google-cloud-storage\", \"google-cloud-dataproc\"]"


with open("template_job.txt", 'r', encoding='utf8') as source:
    lines = source.read()
    lines = lines.replace('@project_id@', project_id)
    lines = lines.replace('@region@', region)
    lines = lines.replace('@cluster_name@', cluster_name)
    lines = lines.replace('@uri_custom_image@', uri_custom_image)
    lines = lines.replace('@properties_file_path@', properties_file_path)
    lines = lines.replace('@main_python_file_uri@', main_python_file_uri)
    lines = lines.replace('@extra_py_file_path@', extra_py_file_path)
    lines = lines.replace('@packages_to_install@', packages_to_install)
    lines = lines.replace('@base_image@', base_image)
    lines = lines.replace('@output_component_file@', output_component_file)

    input_files_paths_string = "["
    for path in input_files_paths:
        input_files_paths_string += f"\"{path}\", "
    input_files_paths_string += "]"

    lines = lines.replace('@input_files_paths@', input_files_paths_string)
    lines = lines.replace('@output_file_path@', output_file_path)
    with open("example.py", 'w', encoding='utf8') as target:
        target.write(lines)
