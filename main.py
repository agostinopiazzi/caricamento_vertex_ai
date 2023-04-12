import pandas as pd
import re
import os
import time
from pathlib import Path


def check_replace(str_to_replace: str, replace_string:str, lines:str) -> str:
    """
    this function replaces a string value inside another string, raising a warning if the value to be replaced
    is not present in the target string.

    :param str_to_replace: the string to be replaced
    :param replace_string: the string to write on the template
    :param lines: the string where the replacement will be made
    :return: the string with the replacement
    """
    if str_to_replace in lines:
        try:
            lines = lines.replace(str_to_replace, replace_string)
        except:
            pass
    else:
        print(f"WARNING: {str_to_replace} not found in template")
        input('continue?')
    return lines

def check_non_replaced_markers(lines):
    # define the pattern
    pattern = r"@\S+?@"

    # search the string
    found_match = re.search(pattern, lines)

    if found_match:
        print(f"WARNING: {found_match.group()} not replaced in template")
        input('continue?')




def clear_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print(f"Folder {folder_path} cleared")


def generate_job_pyfiles(df, job_pyfiles_folder, template_folder_path):

    start = time.time()

    # making a copy of the dataframe
    df = df.copy()

    # changing column names to avoid problems with spaces
    df.columns = [re.sub(' ', '_', col) for col in df.columns]

    # number of jobs
    n_jobs = df.shape[0]

    # defining dataproc variables
    project_id = "bper-ai-ew-test-380011"
    region = "europe-west4"
    cluster_name = "bper-ai-dataproc-cluster"
    uri_custom_image = "europe-west4-docker.pkg.dev/bper-ai-ew-test-380011/nomerepository2/nometag3:latest"
    base_image = "python:3.9"
    packages_to_install = "[\"google-cloud-storage\", \"google-cloud-dataproc\"]"

    # opening the template file for the dataproc jobs
    with open(os.path.join(template_folder_path, "template_job.txt"), 'r', encoding='utf8') as source:

        # initialize the counter of the job
        job_counter = 1

        # iterating over the rows of the dataframe (iterating over every single job / data transform)
        for tpl in df.itertuples():
            try:
                lines = source.read()

                # defining the job name. this name will be used to create the pyfile, the yaml file and will be used as
                # the name of the function that will define the job
                dt_name = tpl.Nome_transform_in_MP
                output_component_file = f"{dt_name}.yaml"
                pyfile_name = dt_name + ".py"
                function_name = dt_name

                # defining file to use to set the spark parameters
                spark_properties_file_name = tpl.Spark_Par + ".txt"

                # getting input list of job
                input_files_paths = tpl.DS_input_list

                # getting output of job
                output_file_path = tpl.Output_dataset_MP

                # replacing the markers in the template with the correct values
                # if a marker does not exist in the template, a warning will be raised
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

                # checking if all the markers have been replaced
                check_non_replaced_markers(lines)

                # writing the pyfile
                with open(os.path.join(job_pyfiles_folder, pyfile_name), 'w', encoding='utf8') as target:
                    target.write(lines)

                print(f'Created {pyfile_name}, job {job_counter} of {n_jobs}')

            except Exception as e:
                print(f'failed to create {pyfile_name} with error {e}')
                input('continue?')

            # incrementing the job counter
            job_counter += 1

            # resetting the pointer to the beginning of the template file
            source.seek(0)

    end = time.time()

    print(f"job pyfiles generated in: {round(end - start, 2)} seconds")


# key --> df_name  value --> task_name

# task_name ---> layer of execution

def generate_pipeline_file(area, df_area, pipeline_pyfile_folder, template_folder_path, pyfile_name):

    # making a copy of the dataframe
    df_area = df_area.copy()

    # changing column names to avoid problems with spaces
    df_area.columns = [re.sub(' ', '_', col) for col in df_area.columns]

    # this variable will be the name of the pipeline on vertex ai
    pipeline_name = area
    # this variable will be the name of the function that will define the pipeline
    pipeline_function_name = area

    # building a dict that associates the name of every output dataframe to the job that creates it
    # todo! attenzione, capita mai che lo stesso df è output di piu jobs?
    support_dict = {}
    for tpl in df_area.itertuples():
        support_dict[tpl.Output_dataset_MP] = tpl.Nome_transform_in_MP

    # opening the template file
    with open(os.path.join(template_folder_path, "template_pipeline.txt"), 'r', encoding='utf8') as source:

        # read the lines as a list of strings (each item is one line)
        lines = source.readlines()

        # building initial lines
        initial_lines = []
        for tpl in df_area.itertuples():
            job_function_name = tpl.Nome_transform_in_MP
            initial_lines.append(f'from ..job_pyfiles.{job_function_name} import {job_function_name}\n')

        # putting things together in the right order
        lines = initial_lines + lines

        # iterating over the rows of the dataframe (iterating over every single job / data transform)
        for tpl in df_area.itertuples():
            job_function_name = tpl.Nome_transform_in_MP
            job_variable_name = job_function_name + '_var'

            # defining job function
            lines.append(f'\n\t# {job_function_name}\n')
            lines.append(f'\t{job_variable_name } = {job_function_name}(\n\t\tinput_folder_path, output_folder_path,'
                         f' main_python_file_folder_path, spark_properties_folder_path, '
                         f'\n\t\thelper_module_file_path\n\t)\n')

            # defining job dependencies
            inputs = tpl.DS_input_list.strip('][').replace("\'", "").split(', ')
            for inp in inputs:
                if inp in support_dict:
                    lines.append(f'\t{job_variable_name}.after({support_dict[inp]})\n')

    # unify all the lines in a single string
    lines = ''.join(lines)

    # replacing the markers in the template with the correct values
    lines = check_replace('@pipeline_name@', pipeline_name, lines)
    lines = check_replace('@pipeline_function_name@', pipeline_function_name, lines)

    # checking if all the markers have been replaced
    check_non_replaced_markers(lines)

    # writing the pyfile
    with open(os.path.join(pipeline_pyfile_folder, pyfile_name), 'w', encoding='utf8') as target:
        target.write(lines)


if __name__ == '__main__':

    # retrieving data from excel Pipeline
    df_dt = pd.read_excel("Pipeline.xlsx", sheet_name="Data Transform MP")
    df_a = pd.read_excel("Pipeline.xlsx", sheet_name="AutomatedSteps")
    df_a.dropna(subset=['DT in MP'], inplace=True)
    df = df_dt.merge(df_a, left_on='Nome transform in MP', right_on='DT in MP', how='left')
    df.fillna('', inplace=True)
    # todo inserire eventuale mappatura tipi per il caricamento da excel

    # defining variables for the creation of the source files for the jobs
    job_pyfiles_folder = './job_pyfiles'
    template_folder_path = './templates'

    # clearing the folder where the pyfiles will be created
    clear_folder(job_pyfiles_folder)

    # generating the pyfiles for the jobs
    generate_job_pyfiles(df, job_pyfiles_folder, template_folder_path)

    # defining variables for the creation of the source files for the pipeline
    pipeline_pyfile_folder = './pipeline_pyfiles'

    # clearing the folder where the pyfiles will be created
    clear_folder(pipeline_pyfile_folder)

    # retrieving the values for the areas
    areas = df['Area'].unique().tolist()

    # iterating over the areas
    for area in areas:

        # retrieving the rows for the current area
        df_area = df.loc[df['Area'] == area, :].copy()

        # generating the pyfile for the pipeline
        generate_pipeline_file(area, df_area, pipeline_pyfile_folder, template_folder_path, f'pipeline_{area}.py')

        print('Generated pipeline file for area', area)