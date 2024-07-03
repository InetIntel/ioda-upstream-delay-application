import bz2
import struct
import ipaddress
import os

import bz2
import pandas as pd
import os

def decompress_and_load_first_n_rows(file_path, n=500):
    with bz2.open(file_path, 'rt') as file:
        df = pd.read_csv(file, delimiter='\t', names=['hex_ip', 'ip', 'history', 'score'], nrows=n)
    return df

def convert_hex_to_binary(hex_string):
    if isinstance(hex_string, str):
        binary_string = ''.join(f'{int(h, 16):04b}' for h in hex_string)
    else:
        binary_string = ''
    return binary_string

def process_data(df):
    df['binary_history'] = df['history'].apply(convert_hex_to_binary)
    

def read_and_process_in_batches(file_path, output_dir, chunk_size=5000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with bz2.open(file_path, 'rt') as file:
        reader = pd.read_csv(file, delimiter='\t', names=['hex_ip', 'ip', 'history', 'score'], chunksize=chunk_size)
        
        for chunk in reader:
            chunk['binary_history'] = chunk['history'].apply(convert_hex_to_binary)
            chunk['first_octet'] = chunk['ip'].apply(lambda x: x.split('.')[0] if isinstance(x, str) else 'unknown')

            for octet, group in chunk.groupby('first_octet'):
                group = group.drop(['hex_ip', 'first_octet'], axis=1)
                output_file_path = f'{output_dir}/ip_{octet}.csv'
                if os.path.exists(output_file_path):
                    group.to_csv(output_file_path, mode='a', header=False, index=False)
                else:
                    group.to_csv(output_file_path, mode='w', header=True, index=False)

                print(f'Appended data to {output_file_path}')


def process_and_print(df):
    df['first_octet'] = df['ip'].apply(lambda x: int(x.split('.')[0]) if isinstance(x, str) else -1)
    for index, row in df.iterrows():
        print(f"Index: {index}, IP: {row['ip']}, First Octet: {row['first_octet']}, History: {row['history']}, Score: {row['score']}")


def address_analysis():
    print("start")
    file_path = 'intermediate_result/targets/internet_address_history_it107w-20240314.fsdb.bz2'
    output_dir = 'intermediate_result/targets/parsed'

    read_and_process_in_batches(file_path, output_dir, 10000)
    