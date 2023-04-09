import pandas as pd
import re
import os
import time


def check_replace(str_to_replace, replace_string, lines):
    if str_to_replace in lines:
        lines = lines.replace(str_to_replace, replace_string)
    else:
        print(f"WARNING: {str_to_replace} not found in template")
        input('continue?')
    return lines


def clear_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print(f"Folder {folder_path} cleared")


def generate_job_pyfiles(df, job_pyfiles_folder, template_folder_path):
    start = time.time()

    project_id = "bper-ai-ew-test-380011"
    region = "europe-west4"
    cluster_name = "bper-ai-dataproc-cluster"
    uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"
    extra_py_file_name = "helper_module.py"

    base_image = "python:3.9"
    packages_to_install = "[\"google-cloud-storage\", \"google-cloud-dataproc\"]"


    # todo valutare questo
    df.fillna("", inplace=True)

    with open(os.path.join(template_folder_path, "template_job.txt"), 'r', encoding='utf8') as source:

        clear_folder(job_pyfiles_folder)

        for tpl in df.itertuples():

            lines = source.read()

            dt_name = tpl.Nome_transform_in_MP
            output_component_file = f"{dt_name}.yaml"
            function_name = dt_name
            pyfile_name = function_name + ".py"
            properties_file_path = tpl.Spark_Par + ".txt"
            input_files_paths = tpl.DS_input_list
            output_file_path = tpl.Output_dataset_MP

            lines = check_replace('@project_id@', project_id, lines)
            lines = check_replace('@region@', region, lines)
            lines = check_replace('@cluster_name@', cluster_name, lines)
            lines = check_replace('@uri_custom_image@', uri_custom_image, lines)
            lines = check_replace('@properties_file_path@', properties_file_path, lines)
            lines = check_replace('@extra_py_file_name@', extra_py_file_name, lines)
            lines = check_replace('@packages_to_install@', packages_to_install, lines)
            lines = check_replace('@base_image@', base_image, lines)
            lines = check_replace('@output_component_file@', output_component_file, lines)
            lines = check_replace('@function_name@', function_name, lines)
            lines = check_replace('@pyfile_name@', pyfile_name, lines)

            with open(os.path.join(job_pyfiles_folder, pyfile_name), 'w', encoding='utf8') as target:
                target.write(lines)

            print(f'Created {pyfile_name}')
            source.seek(0)

    end = time.time()

    print(f"job pyfiles generated in: {round(end - start, 2)} seconds")


if __name__=='__main__':
    df = pd.read_excel("Pipeline.xlsx", sheet_name="Data Transform MP")
    # todo inserire eventuale mappatura tipi per il caricamento da excel
    df.columns = [re.sub(' ', '_', col) for col in df.columns]

    job_pyfiles_folder = './job_pyfiles'
    template_folder_path = './templates'
    generate_job_pyfiles(df, job_pyfiles_folder, template_folder_path)