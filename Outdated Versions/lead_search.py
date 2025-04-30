import pandas as pd
from pathlib import Path
import math
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_csv_files(folder_path: Path, pattern='**/*.csv'):
    return list(folder_path.glob(pattern))

def chunk_files(file_list: list, chunk_division: range) -> Dict[str, list]:
    """Equally divides the list based on chunk size."""
    files_per_chunk = math.ceil(len(file_list) / len(chunk_division))
    chunks = {
        f'{i}': file_list[i * files_per_chunk:(i + 1) * files_per_chunk]
        for i in chunk_division
    }
    return chunks

def chunk_search(file_list: Dict[str, list], chunk_start: int, chunk_end: int, keyword: str, encodings: list, csv_chunk_size: int = 1000000):
    """Search for a keyword within specified chunks of CSV files."""
    search_results = pd.DataFrame()

    processing_list = {
        k: v for k, v in file_list.items() if int(k) in range(chunk_start, chunk_end + 1)
    }

    logging.info(f'Processing chunks: {list(processing_list.keys())}')

    for index, files in processing_list.items():
        logging.info(f'Current Chunk: {index}')
        for file in files:
            logging.info(f'Processing File: {file}')
            for encoding in encodings:
                try:
                    for chunk in pd.read_csv(file, chunksize=csv_chunk_size, on_bad_lines='skip', encoding=encoding):
                        result = chunk[chunk.astype(str).apply(lambda row: keyword in row.values, axis=1)]
                        search_results = pd.concat([search_results, result], ignore_index=True)
                    break  # Stop trying other encodings if successful
                except UnicodeDecodeError:
                    continue  # Try the next encoding

    output_path = Path('Output/plumb_list.csv')
    output_path.parent.mkdir(exist_ok=True, parents=True)
    search_results.to_csv(output_path, index=False)
    logging.info(f'Results saved to {output_path}')

if __name__ == '__main__':
    # Define the parameters
    folder_path = Path('E:/Data')
    chunk_division = range(15)
    keyword = 'plumb'
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']

    # Get CSV files from the folder
    csv_files = get_csv_files(folder_path)
    logging.info(f'Found {len(csv_files)} CSV files.')

    # Chunk the list of CSV files
    chunked_list = chunk_files(csv_files, chunk_division)

    # Perform the search on specified chunks 
    chunk_search(chunked_list, 1, 5, keyword, encodings)