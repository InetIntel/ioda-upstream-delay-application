import os
import json

def count_files_in_directories(directory):
    result = []

    for subdir, dirs, files in os.walk(directory):
        if subdir == directory:
            continue
        print(subdir)
        file_count = len(files)
        result.append({'folder': subdir, 'file_count': file_count})

    return result

def save_results_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

# Specify the directory to scan
directory_path = "/traversa-pool/weili-yarrp-testing/result/venezuela/result"
# Get the list of directories with more than 75000 files
folders_with_many_files = count_files_in_directories(directory_path)
# Save the results to a JSON file
save_results_to_json(folders_with_many_files, 'count.json')

# Print a message indicating the results have been saved
print('Results saved to count.json')