import pandas as pd
import numpy as np
from time import time, sleep
import requests
from nbformat import read, NO_CONVERT
import yaml
from pathlib import Path

# import win32com.client
import os
import shutil
from datetime import datetime
import re
from typing import Optional, Union


def push_notebook_list(
        nb_path_list: list,
        default_push_kwargs: Optional[dict] = None,
        override_push_kwargs: Optional[dict] = None
        
) -> pd.DataFrame:
    """
    This function takes a list holding all the notebooks path references and, after reading the content of each
    notebook, pushes it to the Modeling Platform according to the parameters specified in the push kwargs.
    Push kwargs can be defined inside each notebook using a YAML structure, but they can be specified also in the
    override_push_kwargs or default_push_kwargs arguments if they are not set to None.
    The push parameters held in the former will override the ones specified by the user inside each notebook
    while the parameters held in the latter will be used only if nothing is specified by the user or in the
    override_push_kwargs.

    :param nb_path_list: list: paths where notebooks are stored
    :param creds_dict: dict: holding the credentials to access the project
    :param default_push_kwargs: dict: containing the default push parameters
    :param override_push_kwargs: dict: containing the override push parameters

    :returns: dataframe holding information about the outcome of the process for each notebook
    :rtype: pandas.DataFrame
    """

    # initializing needed variables
    df = pd.DataFrame(columns=['Notebook name', 'Notebook path', 'data transform', 'output var name',
                               'outcome', 'code pushed'])
    proj_dict = {}
    nb_to_upload = len(nb_path_list)
    count = 1
    dt = None
    output_name = None
    code = None

    for nb_path in nb_path_list:

        start = time()  

        try:

            # printing info on notebook
            print('{} / {}'.format(count, nb_to_upload))
            print('printing notebook {}'.format(Path(nb_path).stem))
            print('-------')

            # initializing variables  
            code = []
            output = []
            count_push = 0
            count_config = 0
            push_kwargs = {} if default_push_kwargs is None else default_push_kwargs
            override_push_kwargs = {} if override_push_kwargs is None else override_push_kwargs
            
            # opening notebook
            with open(nb_path, encoding='utf-8') as fp:
                notebook = read(fp, NO_CONVERT)
            cells = notebook['cells']

            # iterating over cells
            for cell in cells:

                # handling code cells
                if cell['cell_type'] == 'code':
                    if cell['source'].startswith(
                            ('# MP_IGNORE',
                             '# PASTE THIS COMMENT AS FIRST LINE OF A CODE CELL TO AVOID EXPORTING IT TO MP')
                    ):
                        continue
                    elif cell['source'].startswith('%'):
                        all_lines = cell['source'].split('\n')
                        if len(all_lines) == 1:
                            continue
                        else:
                            code.append('\n'.join(all_lines[1:]))
                    else:
                        code.append(cell['source'])  # default behaviour --> include

                # handling markdown cells
                elif cell['cell_type'] == 'markdown':
                    if cell['source'].startswith(('# FORCE NON CODE CELL TO EXPORT', '*MP_INCLUDE*', '*MP_INLCUDE*')):
                        source_to_export = '# ' + cell['source'].replace("\n", "\n#")
                        code.append(source_to_export)
                    else:
                        continue  # default behaviour --> ignore

                # handling raw cells
                elif cell['cell_type'] == 'raw':
                    if cell['source'].startswith(('# FORCE NON CODE CELL TO EXPORT', '*MP_INCLUDE*', '*MP_INLCUDE*')):
                        source_to_export = '# ' + cell['source'].replace("\n", "\n#")
                        code.append(source_to_export)
                    elif cell['source'].startswith('# MP_PUSH_CONFIG'):
                        count_config += 1
                        if count_config > 1:
                            print('WARNING: {} mp_config cells detected'.format(count_config))
                        # update push_kwargs
                        read_kwargs = yaml.safe_load(cell['source'])
                        push_kwargs = {**push_kwargs, **read_kwargs}
                        continue
                    elif cell['source'].startswith('# MP_PUSH'):  
                        count_push += 1
                        if count_config == 0:
                            print('WARNING: missing config cell before push cell'.format(count_config))
                        # update push_kwargs. mandatory to specify output variable name in kwargs by the user
                        read_kwargs = yaml.safe_load(cell['source'])
                        final_kwargs = {**push_kwargs, **read_kwargs, **override_push_kwargs}

                        if ('_outputdict' in final_kwargs) and ('_output_dict' not in final_kwargs):
                            print('WARNING: old key output_dict')
                            final_kwargs['_output_dict'] = final_kwargs['_outputdict']

                        # update of project dictionary if needed
                        #proj_dict = update_proj_dict(final_kwargs, proj_dict, client)
                        # pushes code and stores output in memory
                        dt, output_name, outcome = save_code(final_kwargs, proj_dict, code)
                        # save results
                        output_row = [Path(nb_path).stem, nb_path, dt, output_name, outcome, code]
                        output.append(output_row)
                        # moves on
                        continue
                    else:
                        continue  # default behaviour --> ignore

                # should not happen (other kind of cell)      
                else: 
                    continue

            if count_push == 0 and count_config == 0:
                print('missing config tag from notebook {}'.format(Path(nb_path).stem))
                print('missing push tag from notebook {}'.format(Path(nb_path).stem))
                print('-------')
                output_row = [Path(nb_path).stem, nb_path, dt, output_name, 'missing both push and config tags', code]
                output.append(output_row)
            elif count_push == 0:
                print('missing push tag from notebook {}'.format(Path(nb_path).stem))
                print('-------')
                output_row = [Path(nb_path).stem, nb_path, dt, output_name, 'missing push tag', code]
                output.append(output_row)
            else:
                pass

        except Exception as e:
            print('failed during the collection of code from notebook: {}, error: {}, count_push: {}, '
                  'count_config: {}'.format(Path(nb_path).stem, e, count_push, count_config))
            print('-------')
            output_row = [Path(nb_path).stem, nb_path, dt, output_name, e, code]
            output.append(output_row)

        df_partial = pd.DataFrame(output, columns=['Notebook name', 'Notebook path', 'data transform',
                                                   'output var name', 'outcome', 'code pushed'])
        df = pd.concat([df, df_partial], axis=0, ignore_index=True)
        
        count += 1

        # time taken for this notebook
        print('processed in {}'.format(round(time()-start, 2)))
        print('\n ######### \n')

    return df


def fix_code(code: str) -> str:
    """
    This function makes the last fixes on the code, before it is pushed on the Modeling Platform.

    :param code: str: one single string holding all the code lines to be pushed on the related data transform
    :return: the same string with the needed changes
    :rtype: str
    """

    code = re.sub(r"(?m)^%", "#%", code)
    return code


def backup_code(code_string: str, backup_folder_path: str, dt_name: str) -> None:
    """
    This function backs up the string holding the code to be pushed on a .txt file.

    :param code_string: str: one single string holding all the code lines to be pushed on the related data transform
    :param backup_folder_path: str: path of the .txt file where the code is saved
    :param dt_name: str: data transform name

    :return: None
    :rtype: None
    """

    try:
        start = time()
        if not os.path.exists(backup_folder_path):
            os.makedirs(backup_folder_path)
        file_name = dt_name + '.txt'
        with open(os.path.join(backup_folder_path, file_name), 'w', encoding='utf-8') as f:
            f.write(code_string)
        print(f'written backup copy of code on file {file_name} in {backup_folder_path} in {round(time()-start,2)} s')
    except Exception as e:
        print(f'WARNING: backup of dt {dt_name} in {backup_folder_path} failed with error: {e}')


#def update_proj_dict(push_kwargs: dict, proj_dict: dict, client: Client) -> dict:
#    """
#    This function updates the dictionary which holds the references to all the resources of specific projects.
#    When a new project primary key is passed to the push kwargs this function adds the resources of the specific
#    project to proj_dict.
#
#    :param push_kwargs: dict: holding the push parameters
#    :param proj_dict: dict: holding all the resources of the projects of interest
#    :param client: Client: client object of mp_client library
#
#    :return: the updated dictionary
#    :rtype: dict
#    """
#    project_pk = push_kwargs['pj_pk']
#    if project_pk not in proj_dict:
#        project = client.get_project(push_kwargs['pj_pk'])
#        res = project.get_resources()
#        proj_dict[project_pk] = {i.ID: i for i in res}
#    return proj_dict


def save_code(push_kwargs: dict, proj_dict: dict, code: list): # -> tuple[Optional[str], str, Union[str, Exception]]:
    """
    This function pushes the code to the related data transform reporting information about the outcome.

    The function takes the code as a list of string, each element representing a line of code. Then it adds to this list
    the first and last lines of the code (which are Modeling Platform specific), merge all the lines
    (elements of the list) to onw single code string. It makes some final fixings to make it compatible with Modeling
    Platform and execute a backup of it on a .txt file.

    Finally, it tries to push the code to the related data transform. The variable outcome will hold the information
    about the push tentative. Outcome can be a sting with specific information ('ok' if the push was successful) or
    can be any Exception that is raised during the push operation.

    The function returns the outcome variable as well as a string with the name of the data transform where the code has
    been pushed along with the related output name.

    :param push_kwargs: dict: holding the push parameters
    :param proj_dict: dict: holding all the resources of the projects of interest
    :param code: list: holding all the code lines to be pushed, one per each element of the list

    :return: data_transform_name, output_name, outcome
    :rtype: tuple[Optional[str], str, Union[str, Exception]]
    """

    # initializing variables
    data_transform_name = None
    # output name
    output_name = push_kwargs.get('output_name')

    try:
        # writing first lines of code
        first_lines = list()
        first_lines.append('from helper_module import get_input,set_output')
        first_lines.append('import sys')
        first_lines.append('import pyspark')

        first_lines.append('\n')
        first_lines.append('sc = pyspark.SparkContext()')
        for key, value in push_kwargs['_input_dict'].items():
            #first_lines.append('{} = get_input({}, spark=spark)'.format(key, "\"" + value + "_0_0\""))
            first_lines.append(f'{key} = get_input(sc, \'{value}\')')
        #first_lines.append("import sys")
        #first_lines.append('sys.path = [x for x in sys.path if "shr/tmp" not in x]')
        #first_lines.append('from pythokerlib.spark.functions import get_input, set_output')
        #first_lines.append('from pythokerlib.functions import get_parameters')
        #first_lines.append('spark = SparkSession.builder.getOrCreate()')
#
        #first_lines.append('spark.sparkContext.setCheckpointDir("{}")'.format(push_kwargs['checkpoint_path']))
        #first_lines.append('sc = spark.sparkContext')
        #first_lines.append('sc.setLogLevel("WARN")')
        #first_lines.append(r'APPLICATION_GLOBALS = get_parameters("APPLICATION_GLOBALS")')
        #first_lines.append('RUN_ON_MP = APPLICATION_GLOBALS.RUN_ON_MP')
        #first_lines.append('APPLICATION = APPLICATION_GLOBALS.APPLICATION')
        #first_lines.append('CHECKS = APPLICATION_GLOBALS.CHECKS')
        #first_lines.append('app_date = APPLICATION_GLOBALS.APP_DATE')
        first_lines = ['\n'.join(first_lines)]

        # writing last lines of code
        last_lines = list()
        last_lines.append('set_output({})'.format(push_kwargs['output_name']))

        cells_exported = len(code)
        data_transform_name = push_kwargs['_output_dict'][push_kwargs['output_name']]
        outcome = ''

        # turning into string
        final_code = first_lines + code + last_lines
        code_string = '\n\n ################# \n\n'.join(final_code)

        # fix on code
        code_string = fix_code(code_string)
        # backup code
        if 'backup_folder_path' in push_kwargs:

            backup_code(code_string, push_kwargs['backup_folder_path'], data_transform_name)

    except Exception as e:
        print('failed to push code with kwargs {}, error {}'.format(push_kwargs, e))
        outcome = e
        print('-------')
        return data_transform_name, output_name, outcome

    print('-------')
    return data_transform_name, output_name, outcome


