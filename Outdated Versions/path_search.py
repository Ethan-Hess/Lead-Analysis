import pandas as pd
from pathlib import Path
import glob
path = Path('D:/VSCode/Lead Analysis/Data')

def folder_analysis(path):
    '''
    This function will analyze the contents of a folder, and return the number of files and folders.
    '''
    file_count = 0
    folder_count = 0
    for item in path.iterdir():
        # print(file)
        if item.is_file():
            file_count += 1
        elif item.is_dir():
            folder_count += 1
    print(f"CSV Files: {file_count}\nFolder: {folder_count}\n")
    return folder_count, file_count

def data_cleaner(path):
    for item in path.iterdir():
        if item.is_dir():
            print(item)
            folder_count, file_count = folder_analysis(item)
        if folder_count == 0:
            # Create CSV file with # of rows per file
            csv_file_list = glob.glob(f"{item}/*.csv") # CSV files only
            output_path = item

            print(len(csv_file_list), '\n')
            df = pd.DataFrame()

# data_cleaner(path)

folder_breakdown = {
    'folder1': {
        'sub_folders': {
            'path_1': {'sub_folders': {}},
            'path_2': {
                'sub_folder_1': {'sf': {'so on': {0}}, 'csv_files': ['files']}   ,
            },
        },
        'csv_files': ['file1.csv', 'file2.csv'],
    },
    'folder2': {
        'sub_folders': {
            'path_1': {'sub_folders': {}},
            'path_2': {
                'sub_folder_1': {},
            },
        },
        'csv_files': ['file1.csv', 'file2.csv'],
    }
}

'''
dict: Folder(Dict), Files(List)
Repeat for each folder in the path

'''

def dict_builder(path):
    folder_dict = {}
        
    for primary_file in path.iterdir():
        if primary_file.is_dir():
            sub_folder_list = [sub_item for sub_item in primary_file.iterdir() if sub_item.is_dir()]
            csv_file_list = glob.glob(f"{primary_file}/*.csv") # CSV files only
            # Edit it 

            folder_dict[primary_file] = {
                'sub_folders': sub_folder_list,
                'csv_files': csv_file_list
            }


            if sub_folder_list: # updated to call only if the item is a folder
                print(primary_file) # Print the folder name
                for key, value in folder_dict[primary_file].items():   
                    # key: primary_file.path
                    # value: [folder], [csv]
                    
                    print(value)

                        
                    # for sub_folder in folder_dict[item]['sub_folders']:
                    #     print(sub_folder)


                # here is where you change the sub_folder_list to then contain more information:
                # sub_folder: {'PATH': {'sub_folders': {}, 'csv_files': []}}
                # Easy peasie
                temp_dict = {}
                # for sub_folder in sub_folder_list:
                    # print(sub_folder)
        else:
            print(type(primary_file))
            print(primary_file)
    return folder_dict

first_dict = dict_builder(path)

first_key = list(first_dict.keys())[9] # grabs key val in dict


def sub_folder_dict_builder(folder_dict):
    for key, item in folder_dict.items():
        # Triggers if there are sub_folders
        if item['sub_folders']: print(item['sub_folders'])
        # Now go through and build out the sub_folders

def sub(folder):
    pass

# sub_folder_dict_builder(first_dict)
new_path = first_dict[first_key]['sub_folders'][0]
temp_path = Path('D:/VSCode/Lead Analysis/Data/United States/blank_email_united_states') # this path doesn't work??
temp_path = Path('D:/VSCode/Lead Analysis/Data/United States')


test_dict = dict_builder(temp_path)

# put the key of the dictionary into the dict_builder function to return the right values.
# in call, create a new dict that contains the sub folders, and their retrospective parts.
# Alternatively, as we build each of these dictionaries, we can then go through and edit their parts.


# Really I don't need to implement a path search, it would be ideal, but unnecessary.
# Instead I can manually go through each and every folder, build out dataframes, clean data
# Then I can analyze.
