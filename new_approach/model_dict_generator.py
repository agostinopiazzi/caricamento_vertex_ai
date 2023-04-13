import pandas as pd
import sys


def make_model_dict(excel_path, excel_name):

    # read excel
    df = pd.read_excel(excel_path, sheet_name=excel_name)

    # filling missing values in data transform column
    df['Data Transform'].fillna(method='bfill', inplace=True)

    # initializing model dictionary
    model_dict = {}

    # iterating over the rows of the dataframe
    for _, row in df.iterrows():

        # retrieving values from the row
        area = row['Area']
        data_transform = row['Data Transform']
        role = row['Role (I/O)']

        # adding area to model dictionary if not already present
        if area not in model_dict:
            model_dict[area] = {}

        # adding data transform to model dictionary if not already present
        if data_transform not in model_dict[area]:
            job_name = data_transform + '_vertex_name'
            model_dict[area][data_transform] = {
                'inputs': [], 'output': [], 'ancestors': [], 'object': [], 'name': job_name
            }

        # adding input/output to model dictionary
        # object won't be filled here

        # role = input
        if role == 'I':
            # adding input to model dictionary
            model_dict[area][data_transform]['inputs'].append(row['Dataset'])
            # retrieving input ancestor
            df_ancestor = df.loc[(df['Dataset'] == row['Dataset']) & (df['Role (I/O)'] == 'O'), 'Data Transform'].copy()
            # input ancestor, if present,  must be unique
            if df_ancestor.shape[0] > 1:
                print('Error: more than one ancestor found')
                sys.exit(1)
            # adding ancestor to model dictionary
            elif df_ancestor.shape[0] == 1:
                model_dict[area][data_transform]['ancestors'].append(df_ancestor.iloc[0])
            # no ancestor found for this input
            else:
                pass
        # role = output
        elif role == 'O':
            # adding output to model dictionary
            model_dict[area][data_transform]['output'].append(row['Dataset'])
        # role not recognized
        else:
            print('Error: role not recognized')
            sys.exit(1)

    return model_dict


if __name__ == '__main__':
    model_dict = make_model_dict('pipeline_test.xlsx', "Sheet1")
    pass

