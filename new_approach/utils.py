import os
import shutil
def clean_and_re_create(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.makedirs(folder_path)

#def create_or_clean_folder(directory_path):
#    if os.path.exists(directory_path):
#        for file in os.listdir(directory_path):
#            os.remove(os.path.join(directory_path, file))
#    else:
#        os.makedirs(directory_path)