import json
import os
from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud import bigquery
import argparse
import traceback
import re

bq = bigquery.Client()

# Exception class for printing the all queries errors
class BigqueryDeployError(Exception):
    def __init__(self,value):
        self.value = value
    def __str__(self):
        print("################################### SQLS ERRORS #######################################################")
        for i in self.value:
            for key,value in i.items():
                print(f"{key} \n {value}")
                print("-" * 80)
        return repr("Errors in SQLs")
error_list = []

def read_config(path):
    """ Reads the JSON configuration file """

    with open(path) as fp:
        data = json.load(fp)
    fp.close()

    return data

def read_sql(path):
    with open(path) as fp:
        data = fp.read()
    fp.close()

    return data 

def get_replacement_dictionary(config):
    replacements = {}
    for key,value in config['env'].items():
        if key.startswith("v_"):
            replacements[key[2:]] = value
    return replacements

def update_sql(sql,replacements):
    param_pattern = r"\{\{\s*params\.param_\w+\s*\}\}"
    extract_pattern = r"\{\{\s*params\.param_(\w+)\s*\}\}"
    matches = re.findall(param_pattern,sql)

    for match in matches:
        pattern_key = re.findall(extract_pattern,match)[0]
        pattern_value = replacements.get(pattern_key)
        print(f"match: {match} pattern_value {pattern_value}")
        sql = sql.replace(match,pattern_value)
    return sql

def run_query(sql, config):
    
    try:
        sql_query = read_sql(sql)
        replacement_dictionary = get_replacement_dictionary(config)
        sql_query = update_sql(sql_query,replacement_dictionary)
        print(f"Project_id: {os.environ['project_id']}")
        print(f"Running Query: {sql}")
        job = bq.query(sql_query,project=os.environ["project_id"])
        result = job.result()
        print("Job ID: {}".format(job.job_id))
        print("State: {}".format(job.state))
        print("Errors: {}".format(job.errors))
    except Exception as e:
        error = {f"{sql}":e}
        error_list.append(error)

def find_sql_files(root_folder):
    sql_files = []
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))
    return sql_files     

def main():
    """ Entrypoint function to provision resources """

    try:
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--path', type=str, help='Path to the files in the artifacts directory')
        parser.add_argument('--env', type=str, help='Environment: dev, qa or prod')
        parser.add_argument('--domain', type=str, help='Domain: LOB for the data such as hr, pi, etc.')

        args = parser.parse_args()
        print(f"========================= Running for {args.env} Environment =========================")
        
        config_path = os.path.join(args.path, f"{args.domain}_config.json")
        config = read_config(config_path)
        sql_paths = find_sql_files(os.path.join(os.getcwd(), args.path))

        tables = []
        base_views = []
        views = []
        procedures = []
        udfs = []

        for path in sql_paths:
            path_dir = os.path.dirname(path)
            if 'tables' in path_dir:
                tables.append(path)
            elif 'views' in path_dir and 'base_views' in path_dir :
                base_views.append(path)
            elif 'views' in path_dir:
                views.append(path)
            elif 'routines' in path_dir:
                procedures.append(path)
            elif 'udfs' in path_dir:
                udfs.append(path)
        
        print("*************Running queries for tables*********")
        for sql in tables:
            run_query(sql,config)
            
        print("*************Running queries for procedures*********")
        for sql in procedures:
            run_query(sql,config)
        
        print("*************Running queries for udfs*********")
        for sql in udfs:
            run_query(sql,config)

        print("*************Running queries for base views*********")
        for sql in base_views:
            run_query(sql,config)

        print("*************Running queries for views*********")
        for sql in views:
            run_query(sql,config)    
        
        if len(error_list) > 0:
            raise Exception()
        
    except Exception as e:
        if len(error_list) > 0:
            raise BigqueryDeployError(error_list)
        else:
            raise Exception(e)

if __name__ == "__main__":
    main()
