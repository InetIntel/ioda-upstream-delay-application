import pandas as pd
import os
import numpy as np

def get_best_ip(group):
    max_score = group['score'].max()
    best_ips = group[group['score'] == max_score]
    if max_score == 0:
        return best_ips['ip'].iloc[0].rsplit('.', 1)[0] + '.1'
    return best_ips.sample(n=1)['ip'].iloc[0] 

def address_generation():
    input_dir = 'intermediate_result/targets/parsed'
    output_dir = 'intermediate_result/targets/results'
    
    for first_octet in range(1, 256): 
        file_name = f"ips_{first_octet}.csv"
        file_path = os.path.join(input_dir, file_name)
        output_path = os.path.join(output_dir, file_name)

        if os.path.exists(file_path):
            data = pd.read_csv(file_path)
            print(f"file_name {file_name}")
        
            data['prefix24'] = data['ip'].apply(lambda x: '.'.join(x.split('.')[:3]) + '.0/24')
            max_score_per_prefix = data.groupby('prefix24').apply(get_best_ip).reset_index()
            max_score_per_prefix.columns = ['prefix24', 'best_ip']
            
            max_score_per_prefix.to_csv(output_path, mode='w', header=False, index=False)
    
    return 
