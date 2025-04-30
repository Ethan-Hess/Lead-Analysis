import pandas as pd
from pathlib import Path
import glob
path = Path('D:/VSCode/Lead Analysis/Data')


'''
The objective of this program is to clean the data, making breakdowns subfolder by subfolder, comparing data with self to ensure no duplicates, store cleaned data, filter based on key, and return new database.

TODO: 
- Clean data based on subfolder
- Compare data with self to ensure no duplicates
- Store cleaned data
- Filter based on key
- Return new database
'''

'''
for folder in data:
    for file in folder:
        read file # of rows
        clean file
        compare with self
        store cleaned data
        filter based on key
        return new database


Make output folder for cleaned data, for each subfolder
search through each 

'''

# -------------------------------------------------------------------------------------------------------
# For now, we will cross reference the data files, to see if they are derived from the same source
def list_file_types_in_directory(directory_path):
    path = Path(directory_path)
    file_types_count = {}
    for p in path.glob('**/*'):
        if p.is_file():
            suffix = p.suffix
            if suffix in file_types_count:
                file_types_count[suffix] += 1
            else:
                file_types_count[suffix] = 1
    return file_types_count


directory_path = 'D:/VSCode/Lead Analysis/Data'
# file_types_count = list_file_types_in_directory(directory_path)
# print(f"File types and their counts in directory '{directory_path}': {file_types_count}")
# -------------------------------------------------------------------------------------------------------

# Read file headers
def read_file_headers(file_path, encodings):
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, nrows=0)
            return df.columns.tolist(), encoding
        except Exception as e:
            continue
    return None, None

# Count file rows

def count_file_rows(file_path, encoding):
    chunk_size = 1000000
    row_count = 0
    try:
        for chunk in pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size):
            row_count += len(chunk)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return row_count

# Make a program that will go through each folder in Data, and run an analysis on that data

def file_header_analysis(directory_path):
    path = Path(directory_path)
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    
    df = pd.DataFrame(columns=['Filename', '# of rows', 'Column List'])
    
    for folder in path.iterdir():
        # print(folder)
        if folder.is_dir():
            print(f"Analyzing folder: {folder}")
            csv_file_list = glob.glob(f"{folder}/*.csv") # CSV files only
            print(csv_file_list)
            # for csv_file in csv_file_list:
            #     print(f"Analyzing file: {csv_file}")
            #     columns, encoding = read_file_headers(csv_file, encodings)
            #     if columns is None:
            #         print(f'Failed to decode {csv_file} with available encodings.')
            #         continue
            #     row_count = count_file_rows(csv_file, encoding, chunk_size)
            #     column_list = ', '.join(columns)
            #     print(f"Filename: {csv_file}, # of rows: {row_count}, Column List: {column_list}")
    #             data = {
    #                 'Filename': [csv_file],
    #                 '# of rows': [row_count],
    #                 'Column List': [columns]
    #             }
    #             df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    # df.to_csv('temp_analysis.csv', index=False)

# file_header_analysis(directory_path)
def folder_analysis(path):
    csv_count = 0
    folder_count = 0
    for item in path.iterdir():
        # print(file)
        if item.is_file():
            csv_count += 1
        elif item.is_dir():
            folder_count += 1
    print(f"CSV Files: {csv_count}\nFolder: {folder_count}\n")
    return folder_count, csv_count




def data_cleaner(path):
    for item in path.iterdir():
        if item.is_dir():
            print(item)
            folder_count, csv_count = folder_analysis(item)
        if folder_count == 0:
            # Create CSV file with # of rows per file
            csv_file_list = glob.glob(f"{item}/*.csv") # CSV files only
            output_path = item

            print(csv_file_list, '\n')
            df = pd.DataFrame()
    pass

data_cleaner(path)