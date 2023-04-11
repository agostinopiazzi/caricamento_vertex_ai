import pandas as pd
import re
import os
import time
from pathlib import Path


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
    df.columns = [re.sub(' ', '_', col) for col in df.columns]

    project_id = "bper-ai-ew-test-380011"
    region = "europe-west4"
    cluster_name = "bper-ai-dataproc-cluster"
    uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"



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
            spark_properties_file_name = tpl.Spark_Par + ".txt"
            input_files_paths = tpl.DS_input_list
            output_file_path = tpl.Output_dataset_MP

            lines = check_replace('@project_id@', project_id, lines)
            lines = check_replace('@region@', region, lines)
            lines = check_replace('@cluster_name@', cluster_name, lines)
            lines = check_replace('@uri_custom_image@', uri_custom_image, lines)
            lines = check_replace('@spark_properties_file_name@', spark_properties_file_name, lines)
            lines = check_replace('@packages_to_install@', packages_to_install, lines)
            lines = check_replace('@base_image@', base_image, lines)
            lines = check_replace('@output_component_file@', output_component_file, lines)
            lines = check_replace('@function_name@', function_name, lines)
            lines = check_replace('@pyfile_name@', pyfile_name, lines)
            lines = check_replace('@input_files_paths@', input_files_paths, lines)
            lines = check_replace('@output_file_path@', output_file_path, lines)

            with open(os.path.join(job_pyfiles_folder, pyfile_name), 'w', encoding='utf8') as target:
                target.write(lines)

            print(f'Created {pyfile_name}')
            source.seek(0)

    end = time.time()

    print(f"job pyfiles generated in: {round(end - start, 2)} seconds")


# key --> df_name  value --> task_name

# task_name ---> layer of execution

def generate_pipeline_file(df_area, pipeline_pyfile_folder, template_folder_path, pyfile_name):

    df.columns = [re.sub(' ', '_', col) for col in df.columns]

    pipeline_name = 'test_pipeline'
    pipeline_function_name = 'test_pipeline_function'
    # build support dict
    support_dict = {}
    for tpl in df_area.itertuples():
        support_dict[tpl.Output_dataset_MP] = tpl.Nome_transform_in_MP

    with open(os.path.join(template_folder_path, "template_pipeline.txt"), 'r', encoding='utf8') as source:
        lines = source.readlines()

        initial_lines = []

        for tpl in df_area.itertuples():
            job_function_name = tpl.Nome_transform_in_MP
            initial_lines.append(f'from ..job_pyfiles import {job_function_name}\n')
        initial_lines.append('\n\n')
        lines = initial_lines + lines

        for tpl in df_area.itertuples():
            job_function_name = tpl.Nome_transform_in_MP
            job_variable_name = job_function_name +'_var'
            lines.append(f'\n\t# {job_function_name}\n')
            lines.append(f'\t{job_variable_name } = {job_function_name}(input_folder_path, output_folder_path,'
                         f' main_python_file_folder_path, spark_properties_folder_path, helper_module_file_path)\n')
            inputs = tpl.DS_input_list.strip('][').replace("\'","").split(', ')
            for input in inputs:
                if input in support_dict:
                    lines.append(f'\t{job_variable_name}.after({support_dict[input]})\n')

    lines = ''.join(lines)

    lines = check_replace('@pipeline_name@', pipeline_name, lines)
    lines = check_replace('@pipeline_function_name@', pipeline_function_name, lines)

    with open(os.path.join(pipeline_pyfile_folder, pyfile_name), 'w', encoding='utf8') as target:

        target.write(lines)















if __name__=='__main__':
    df = pd.read_excel("Pipeline.xlsx", sheet_name="Data Transform MP")
    # todo inserire eventuale mappatura tipi per il caricamento da excel


    job_pyfiles_folder = './job_pyfiles'
    template_folder_path = './templates'
    #generate_job_pyfiles(df, job_pyfiles_folder, template_folder_path)

    pipeline_pyfile_folder = './pipeline_pyfiles'

    df_dataset = pd.read_excel("Pipeline.xlsx", sheet_name="Dataset MP")
    df_dataset = df_dataset.loc[df_dataset['Area'] == 'Input BPER', :].copy()
    start_inputs = df_dataset['Dataset'].unique().tolist()
    start_inputs = [Path(i).stem for i in start_inputs]

    clear_folder(pipeline_pyfile_folder)


    generate_pipeline_file(df, pipeline_pyfile_folder, template_folder_path, 'pipeline_test.py')