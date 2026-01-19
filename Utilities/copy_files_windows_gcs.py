# !gcloud config set project hca-hin-dev-cur-parallon

import os
import re
import yaml
import json
import subprocess
import logging
os.environ["HTTP_PROXY"] = "proxy.nas.medcity.net:80"
os.environ["HTTPS_PROXY"] = "proxy.nas.medcity.net:80"

excel_files_path_folder = r"C:\Merwin\Corrections\Para\Excel Files\Jun06"
gcs_bucket = "eim-parallon-cs-datamig-dev-0003"
gcs_parent_directory = "edwradata/srcfiles/govreports"
input_file_extension = "xlsx"

file_patterns_config = r"InputFiles\file_patterns_all.yaml"

def run_gsutil_command(source, destination):
    command = f'gsutil -m cp -r "{source}" {destination}'
    print(command)
    try:
        completed_process = subprocess.run(
            command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(completed_process.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e)
        print(e.output)

with open(file_patterns_config, 'r') as file_patt_conf:
    file_patterns_yaml = yaml.safe_load(file_patt_conf)
    file_patterns_json = json.loads(json.dumps(file_patterns_yaml, default=str))

for sub_directory in file_patterns_json['processing_groups']:
    print(sub_directory)
    file_list = [filename for filename in os.listdir(f"{excel_files_path_folder}\{sub_directory}") if filename.endswith(input_file_extension)]

    for file_pattern in file_patterns_json["file_patterns"]:
        
        destination_gcs_path = f"gs://{gcs_bucket}/{gcs_parent_directory}/{sub_directory.lower()}/{input_file_extension}/"
        source_local_path = f"{excel_files_path_folder}\{sub_directory}\{file_pattern['file_pattern_terminal']}"
        run_gsutil_command(source_local_path, destination_gcs_path)

        # print(file_pattern["file_pattern_python"])
        # file_count = 0
        # for filename in file_list:
        #     is_pattern = re.search(file_pattern["file_pattern_python"], filename, re.IGNORECASE)
        #     if is_pattern:
        #         file_count += 1
        # print(file_count)