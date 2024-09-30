import pandas as pd
from pathlib import Path
import numpy as np
import glob, os, math

folder_path = 'E:\Data'
csv_files = glob.glob(os.path.join(folder_path, '**', '*.csv'), recursive=True)


encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
keyword = 'plumb'

chunk_division = range(15)
temp_files = list(range(500))
csv_chunk_size = 1000000
filtered_chunks = []


def chunk_files(file_list): # Equally divides the list based off of chunk size
    files_per_chunk = math.ceil(len(file_list) / len(chunk_division))
    chunk = {}
    x = 0
    for i in chunk_division:
        y = x + files_per_chunk if i < max(chunk_division) else len(file_list)
        chunk[f'{i}'] = file_list[x:y]
        x = y
    return chunk

def chunk_search(file_list:dict, chunk_start, chunk_end):
    search_results = pd.DataFrame()
    processing_list = {}

    # inclusive range of file index
    if chunk_start == chunk_end:
        processing_list = file_list[f'{chunk_start}']
    else:
        processing_range = range(chunk_start, chunk_end + 1)
        for i in processing_range:
            print(i)
            processing_list[f'{i}'] = file_list[f'{i}']
    print(file_list)
    print(type(processing_list))


    for index, files in processing_list.items():
        print(f'Current Chunk: {index}')
        for file in files:
            print(f'\nCurrent File: {file}\n')
            for encoding in encodings:
                try:
                    for chunk in pd.read_csv(file, chunksize=1000000, on_bad_lines='skip', encoding=encoding):
                        result = chunk[chunk.apply(lambda row: row.astype(str).str.contains('plumb').any(), axis=1)]
                        search_results = pd.concat([search_results, result], ignore_index=True)
                    break  # If it succeeds, no need to try other encodings
                except UnicodeDecodeError:
                    continue  # Try the next encoding

    search_results.to_csv('Output\plumb_list.csv', index=False) #16.49 to search whole dataset

# greg = {}
# greg[1] = csv_files
# print(greg)
# chunk_search(greg, 1, 1) # inclusive
print(len(csv_files))
chunked_list = chunk_files(csv_files)
chunk_search(chunked_list, 1, 5)