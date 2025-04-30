import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_csv_files(folder_path: Path, pattern: str = '**/*.csv') -> List[Path]:
    """Returns a list of CSV file paths matching the given pattern."""
    return list(folder_path.glob(pattern))

def read_file_headers(file: Path, encodings: List[str]) -> Optional[pd.Index]:
    """Reads the headers of a file using different encodings and returns the columns."""
    for encoding in encodings:
        try:
            columns_df = pd.read_csv(file, nrows=0, encoding=encoding)
            return columns_df.columns, encoding
        except UnicodeDecodeError:
            continue
    return None, None

def count_file_rows(file: Path, encoding: str, chunk_size: int = 1000000) -> int:
    """Counts the number of rows in a CSV file."""
    row_count = 0
    for chunk in pd.read_csv(file, chunksize=chunk_size, usecols=[0], on_bad_lines='skip', encoding=encoding):
        row_count += len(chunk)
    return row_count

def analyze_files(csv_files: List[Path], encodings: List[str], chunk_size: int = 1000000) -> pd.DataFrame:
    """Analyzes CSV files for row count and column information."""
    leads_breakdown = pd.DataFrame(columns=['Filename', '# of rows', 'Column List'])

    for file in csv_files:
        name = file.name
        logging.info(f'Processing file: {name}')
        
        columns, encoding = read_file_headers(file, encodings)
        if columns is None:
            logging.warning(f'Failed to decode {name} with available encodings.')
            continue
        
        row_count = count_file_rows(file, encoding, chunk_size)
        column_list = ', '.join(columns)
        
        new_row = pd.DataFrame([{'Filename': name, '# of rows': row_count, 'Column List': column_list}])
        leads_breakdown = pd.concat([leads_breakdown, new_row], ignore_index=True)
        logging.info(f'Done processing {name}: {row_count} rows.')
    
    return leads_breakdown

if __name__ == '__main__':
    # Define parameters
    folder_path = Path('D:/VSCode/Lead Analysis')
    output_path = Path('Output/lead_analysis.csv')
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    csv_chunk_size = 1000000
    
    # Gather CSV files
    csv_files = get_csv_files(folder_path)
    logging.info(f'Found {len(csv_files)} CSV files.')
    
    # Analyze files for row count and columns
    leads_breakdown = analyze_files(csv_files, encodings, csv_chunk_size)
    
    # Save results to CSV
    output_path.parent.mkdir(exist_ok=True, parents=True)
    leads_breakdown.to_csv(output_path, index=False)
    logging.info(f'Results saved to {output_path}')