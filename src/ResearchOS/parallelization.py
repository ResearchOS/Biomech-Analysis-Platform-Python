import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import fcntl  # Use `import msvcrt` on Windows
import time

def locked_write_parquet(file_path: str, df: pd.DataFrame, mode: str = 'w', timeout: float = 10):
    """
    Write a DataFrame to a Parquet file with a file lock to prevent concurrent writes.

    Parameters:
    - file_path (str): The path to the Parquet file.
    - df (pd.DataFrame): The DataFrame to write to the Parquet file.
    - mode (str): The mode in which to open the file ('w' for write, 'a' for append). Default is 'w'.
    - timeout (int): Timeout in seconds to wait for acquiring the lock. Default is 10 seconds.
    """
    start_time = time.time()
    
    # Open the file with the desired mode
    with open(file_path, 'wb' if mode == 'w' else 'ab') as file:
        while True:
            try:
                # Acquire an exclusive lock (LOCK_EX)
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                print(f"Lock acquired for writing to {file_path}")
                
                # Convert DataFrame to Parquet and write
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path)
                
                fcntl.flock(file, fcntl.LOCK_UN)  # Release the lock
                print(f"Lock released after writing to {file_path}")
                break
            except BlockingIOError:
                # If the lock is not acquired, wait for a bit and retry
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Could not acquire lock for writing to {file_path} within {timeout} seconds.")
                print(f"Waiting to acquire lock for writing to {file_path}...")
                time.sleep(0.1)  # Sleep a bit before retrying

def locked_read_parquet(file_path: str, timeout: float = 10):
    """
    Read data from a Parquet file with a file lock to prevent concurrent reads/writes.

    Parameters:
    - file_path (str): The path to the Parquet file.
    - timeout (int): Timeout in seconds to wait for acquiring the lock. Default is 10 seconds.

    Returns:
    - pd.DataFrame: The DataFrame read from the Parquet file.
    """
    start_time = time.time()
    
    # Open the file in read mode
    with open(file_path, 'rb') as file:
        while True:
            try:
                # Acquire a shared lock (LOCK_SH)
                fcntl.flock(file, fcntl.LOCK_SH | fcntl.LOCK_NB)
                print(f"Lock acquired for reading from {file_path}")
                
                # Read Parquet data into a DataFrame
                df = pd.read_parquet(file_path)
                
                fcntl.flock(file, fcntl.LOCK_UN)  # Release the lock
                print(f"Lock released after reading from {file_path}")
                return df
            except BlockingIOError:
                # If the lock is not acquired, wait for a bit and retry
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Could not acquire lock for reading from {file_path} within {timeout} seconds.")
                print(f"Waiting to acquire lock for reading from {file_path}...")
                time.sleep(0.1)  # Sleep a bit before retrying
