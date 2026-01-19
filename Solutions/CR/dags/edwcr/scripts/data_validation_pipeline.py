import argparse
import logging
import yaml
import os
import shutil
import sys

import apache_beam as beam
from apache_beam.io import WriteToText

from apache_beam.options.pipeline_options import PipelineOptions

from google.cloud import bigquery
import logging
from google.cloud import bigquery
from mako.template import Template
from mako import exceptions
import datetime
import json
from google.cloud import storage

#destn_cfg_file_name = 'config.yml'

def download_gcs_file(input_file):
    storage_client = storage.Client()    
    bucket_name = input_file.split("/")[2]
    source_blob_name = "/".join(input_file.split("/")[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    downloaded_blob_name = os.path.join(base_dir, source_blob_name.replace('/', '_'))
    blob.download_to_filename(downloaded_blob_name)
    logging.info(
        "Input file {} downloaded to file {}. successfully ".format(
            input_file, downloaded_blob_name
        )
    )
    return downloaded_blob_name
    

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    logging.info(
        "file {} downloaded to file path {}. successfully ".format(
            source_blob_name, destination_file_name
        )
    )

class IdentifyTablesDoFn(beam.DoFn):

    def __get_table_starts_with(self, project_id, dataset, prefix, suffix):
        prefix_tables = []
        client = bigquery.Client(project = project_id)
        full_dataset_id = "{}.{}".format(project_id, dataset)
        tables = client.list_tables(full_dataset_id)  
        if tables:
            for table in tables:
                t_name = table.table_id
                qualifies = False
                if prefix != '':
                    if t_name.startswith(prefix):
                        if suffix != '':
                            if t_name.endswith(suffix):
                                qualifies = True
                        else:
                            qualifies = True
                elif suffix != '':
                    if t_name.endswith(suffix):
                        qualifies = True  
                else:
                    qualifies = True
                if qualifies:
                    prefix_tables.append(t_name)
        logging.info('prefix_tables :' + str(prefix_tables))
        return prefix_tables


    def process(self, element):
        tables_config = []
        src_project_id = element['src_project_id']
        tgt_project_id = element['tgt_project_id']
        src_dataset = element['src_dataset']
        tgt_dataset = element['tgt_dataset']
        remarks = element['remarks']
        result_dataset = element['result_dataset']
        result_table = element['result_table']

        exclude_tables = []
        if 'exclude-tables' in element:
            exclude_tables = element['exclude-tables']
        
        if 'tables' not in element:
            client = bigquery.Client(project = tgt_project_id)
            full_dataset_id = "{}.{}".format(tgt_project_id, tgt_dataset)
            tables = client.list_tables(full_dataset_id)  
            if tables:
                for table in tables:
                    if table.table_id not in exclude_tables:
                        tables_config.append({
                            'src_project_id': src_project_id, 
                            'tgt_project_id': tgt_project_id, 
                            'src_dataset': src_dataset, 
                            'tgt_dataset': tgt_dataset, 
                            'table': table.table_id, 
                            'remarks': remarks,
                            'result_dataset': result_dataset,
                            'result_table': result_table
                            })
        else:
            tables = element['tables']
            tables_unique_list = []
            if tables:
                meta = {
                            'src_project_id': src_project_id, 
                            'tgt_project_id': tgt_project_id, 
                            'src_dataset': src_dataset, 
                            'tgt_dataset': tgt_dataset, 
                            'remarks': remarks,
                            'result_dataset': result_dataset,
                            'result_table': result_table
                            }
                for table in tables:
                    if table['name'] in exclude_tables or table['name'] in tables_unique_list:
                        continue
                    if '*' in table['name']:
                        w = table['name'].strip()
                        arr = w.split('*')
                        tbls = self.__get_table_starts_with(tgt_project_id, tgt_dataset, arr[0], arr[1])
                        for t in tbls:
                            if t in exclude_tables or t in tables_unique_list:
                                continue
                            tables_unique_list.append(t)
                            obj = {'table': t}
                            obj.update(meta)
                            if 'src-filter' in table:
                                obj['src_filter'] = table['src-filter']
                            if 'tgt-filter' in table:
                                obj['tgt_filter'] = table['tgt-filter']
                            if 'order-by' in table:
                                obj['order_by'] = table['order-by']
                            if 'exclude-columns' in table:
                                obj['exclude_columns'] = table['exclude-columns']
                            tables_config.append(obj)
                    else:
                        tables_unique_list.append(table['name'])
                        obj = {'table': table['name']}
                        obj.update(meta)
                        if 'src-filter' in table:
                            obj['src_filter'] = table['src-filter']
                        if 'tgt-filter' in table:
                            obj['tgt_filter'] = table['tgt-filter']
                        if 'order-by' in table:
                            obj['order_by'] = table['order-by']
                        if 'exclude-columns' in table:
                            obj['exclude_columns'] = table['exclude-columns']
                        tables_config.append(obj)
        
        logging.info('============tables_config :' + str(tables_config))
        return tables_config

class ExploreTableDefinitionDoFn(beam.DoFn):
    def process(element):
        src_project_id = element['src_project_id']
        tgt_project_id = element['tgt_project_id']
        src_dataset = element['src_dataset']
        tgt_dataset = element['tgt_dataset']
        table_name = element['table']
        remarks = element['remarks']
        result_dataset = element['result_dataset']
        result_table = element['result_table']
        
        exclude_columns = []
        if 'exclude_columns' in element:
            exclude_columns = element['exclude_columns']

        client = bigquery.Client(project = tgt_project_id)
        full_table_id = '{}.{}.{}'.format(tgt_project_id, tgt_dataset, table_name)
        table = client.get_table(full_table_id)
        columns = []
        key_column = None
        if table.schema:
            for column in table.schema:
                if key_column is None:
                    key_column = column.name
                if column.name not in exclude_columns:
                    columns.append({'name':column.name, 'field_type': column.field_type})
        obj = {
            'src_project_id': src_project_id, 
            'tgt_project_id': tgt_project_id, 
            'src_dataset': src_dataset, 
            'tgt_dataset': tgt_dataset, 
            'table': table_name,
            'columns': columns,
            'key_column': key_column,
            'remarks': remarks,
            'result_dataset': result_dataset,
            'result_table': result_table
        }
        if 'src_filter' in element:
            obj['src_filter'] = element['src_filter']
        if 'tgt_filter' in element:
            obj['tgt_filter'] = element['tgt_filter']
        if 'order_by' in element:
            obj['order_by'] = element['order_by']
        #logging.info('============obj :' + str(obj))
        return obj

class ValidateCountDoFn(beam.DoFn):
    def process(element):
        # logging.info(element)
        
        download_blob(env_config['env']["v_dag_bucket_name"],'dags/sql/data_validation/validate_count_query_template.sql', 'validate_count_query_template.sql')
        query = ''
        try:
            mytemplate = Template(filename='validate_count_query_template.sql', 
                                  module_directory='/tmp/mako_modules')
            query = mytemplate.render(table_config=element)
            logging.info('============query :' + query)

        except:
            logging.error(exceptions.text_error_template().render())

        client = bigquery.Client(project = element['tgt_project_id'])
        query_job = client.query(query)
        passed = True
        if query_job.errors:
            element['errors'] = query_job.errors
            logging.error(query_job.errors)
            passed = False
        else:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
            logging.info(now_str)
            meta = {
                    'source_project': element['src_project_id'],
                    'target_project': element['tgt_project_id'],
                    'source_dataset': element['src_dataset'],
                    'target_dataset': element['tgt_dataset'],
                    'table': element['table'],
                    'comparison': 'count',
                    'validation_time': now_str,
                    'remarks': element['remarks']}
            results = []
            
            for row in query_job:
                src_count = row['source']
                if src_count is None:
                    src_count = -1
                tgt_count = row['target']
                if tgt_count is None:
                    tgt_count = -1
                count = (src_count - tgt_count)
                if passed and count != 0:
                        passed = False
                obj = {'metric': 'No. of missing rows', 'data': count}
                obj.update(meta)
                results.append(obj)
            
            logging.info(results)
            result_table_id = '{}.{}.{}'.format(element['tgt_project_id'], 
                                                element['result_dataset'], element['result_table'])
            errors = client.insert_rows_json(result_table_id, results)
            if errors == []:
                logging.info("Added Rows to Result Table.")
            else:
                logging.error("Encountered errors while inserting rows into result table: {}".format(errors))
            
        if not passed:
            element['query'] = query
            return beam.pvalue.TaggedOutput('count_mismatch', element)
       
        return element

class ValidateColumnsDoFn(beam.DoFn):
    def process(element):
        # logging.info(element)
        download_blob( env_config['env']["v_dag_bucket_name"],'dags/sql/data_validation/validate_columns_query_template.sql', 'validate_columns_query_template.sql')
        query = ''
        try:
            mytemplate = Template(filename='validate_columns_query_template.sql', 
                                  module_directory='/tmp/mako_modules')
            query = mytemplate.render(table_config=element)
            logging.info(query)
        except:
            logging.error(exceptions.text_error_template().render())

        client = bigquery.Client(project = element['tgt_project_id'])
        query_job = client.query(query)
        passed = True
        if query_job.errors:
            element['errors'] = query_job.errors
            logging.error(query_job.errors)
            passed = False
        else:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
            meta = {
                    'source_project': element['src_project_id'],
                    'target_project': element['tgt_project_id'],
                    'source_dataset': element['src_dataset'],
                    'target_dataset': element['tgt_dataset'],
                    'table': element['table'],
                    'comparison': 'columns mismatch',
                    'validation_time': now_str,
                    'remarks': element['remarks']}
            results = []
            passed = True
            for row in query_job:
                for column in element['columns']:
                    count = row['non_matching_'+column['name']]
                    if count is None:
                        count = 0
                    if passed and count != 0:
                        passed = False
                    obj = {'metric': column['name'], 'data': count}
                    obj.update(meta)
                    results.append(obj)
            logging.info(results)

            result_table_id = '{}.{}.{}'.format(element['tgt_project_id'], 
                                                element['result_dataset'], element['result_table'])
            errors = client.insert_rows_json(result_table_id, results)
            if errors == []:
                logging.info("New rows have been added.")
            else:
                logging.error("Encountered errors while inserting rows: {}".format(errors))
        
        if not passed:
            return beam.pvalue.Tadownload_blobshuggedOutput('columns_mismatch', element)
       
        return element

class ValidateRowsDoFn(beam.DoFn):
    def process(element):
        # logging.info(element)
        download_blob(env_config['env']["v_dag_bucket_name"],'dags/sql/data_validation/validate_rows_query_template.sql', 'validate_rows_query_template.sql')
        query = ''
        try:
            mytemplate = Template(filename='validate_rows_query_template.sql', 
                                  module_directory='/tmp/mako_modules')
            query = mytemplate.render(table_config=element)
            logging.info(query)
        except:
            logging.error(exceptions.text_error_template().render())
            
        client = bigquery.Client(project = element['tgt_project_id'])
        query_job = client.query(query)
        passed = True
        if query_job.errors:
            element['errors'] = query_job.errors
            logging.error(query_job.errors)
            passed = False
        else:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
            meta = {
                    'source_project': element['src_project_id'],
                    'target_project': element['tgt_project_id'],
                    'source_dataset': element['src_dataset'],
                    'target_dataset': element['tgt_dataset'],
                    'table': element['table'],
                    'comparison': 'rows mismatch',
                    'validation_time': now_str,
                    'remarks': element['remarks']}
            results = []
            passed = True
            for row in query_job:
                count = row['mismatch_rows']
                if passed and count != 0:
                        passed = False
                obj = {'metric': 'No. of rows not matching', 'data': count}
                obj.update(meta)
                results.append(obj)

            result_table_id = '{}.{}.{}'.format(element['tgt_project_id'], 
                                                element['result_dataset'], element['result_table'])
            errors = client.insert_rows_json(result_table_id, results)
            if errors == []:
                logging.info("New rows have been added.")
            else:
                logging.error("Encountered errors while inserting rows: {}".format(errors))
            
            if not passed:
                element['query'] = query
                return beam.pvalue.TaggedOutput('rows_mismatch', element)
            logging.info(results)
       
        return element  

class ReportFailedTableDoFn(beam.DoFn):
    def process(element):
        qry = ''
        err = ''

        if 'query' in element:
            qry = ', Query to validate - {0}'.format(element['query'])

        if 'errors' in element:
            err = ', Error - {0}'.format(element['errors'])

        logging.error('Table validation failed [{0}.{1}.{4} == {2}.{3}.{4}] {5}{6}'.format(
            element['src_project_id'], element['src_dataset'], 
            element['tgt_project_id'], element['src_dataset'], 
            element['table'], qry, err))

def __identify_datasets(config):
    logging.info('Identify Configuration')

    datasets_config = []
    src_project_id = config['project']['src-project-id']
    tgt_project_id = config['project']['tgt-project-id']
    remarks = 'Not provided'
    if 'remarks' in config:
        remarks = config['remarks']
    result_dataset = config['result']['dataset']
    result_table = config['result']['table']

    result_table_schema = [
        bigquery.SchemaField("source_project", "STRING"),
        bigquery.SchemaField("source_dataset", "STRING"),
        bigquery.SchemaField("target_project", "STRING"),
        bigquery.SchemaField("target_dataset", "STRING"),
        bigquery.SchemaField("table", "STRING"),
        bigquery.SchemaField("comparison", "STRING"),
        bigquery.SchemaField("metric", "STRING"),
        bigquery.SchemaField("data", "NUMERIC"),
        bigquery.SchemaField("remarks", "STRING"),   
        bigquery.SchemaField("validation_time", "DATETIME"),         
    ]
    
    client = bigquery.Client(project = dv_config['project']['tgt-project-id'])
    table_ref = client.dataset(result_dataset).table(result_table)

    try:
        table = bigquery.Table(table_ref, schema=result_table_schema)
        table = client.create_table(table)
        logging.info("Table {} created successfully.".format(config['result']['table']) )
    except Exception as e:
        if "Already Exists" in str(e):
            logging.info("Table {} already exists, No need to create".format(config['result']['table']) )
        else:
            logging.info("Error while creating table {}".format(e))

   
    datasets = config['project']['datasets']
    for dataset in datasets:
        obj = {
            'src_project_id': src_project_id, 
            'tgt_project_id': tgt_project_id, 
            'src_dataset': dataset['src-dataset'], 
            'tgt_dataset': dataset['tgt-dataset'], 
            'remarks': remarks,
            'result_dataset': result_dataset,
            'result_table': result_table
            }
        if 'exclude-tables' in dataset:
            obj['exclude-tables'] = dataset['exclude-tables']
        if 'tables' in dataset:
            obj['tables'] = dataset['tables']
        datasets_config.append(obj)

    logging.info('Config loaded')
    logging.info(datasets_config)

    return datasets_config

def __load_config(config_file):
    global config 
    config = None
    if config_file.startswith("gs://"):  
        downloaded_blob_name = download_gcs_file(config_file)    
        with open(os.path.join(base_dir, downloaded_blob_name), 'r') as file:   
            config = yaml.safe_load(file)
            logging.info("Reading Config file downloaded from GCS {}".format(downloaded_blob_name))
    else:
        with open(os.path.join(config_folder, config_file), 'r') as file:
            config = yaml.safe_load(file)
            logging.info("Reading Local Config file {}".format(config_folder + config_file))
    return config

def run(argv=None):
    pipeline_args = [
        "--project", env_config['env']['v_proc_project_id'],
        "--service_account_email",  env_config['env']['v_serviceaccountemail'],
        "--job_name", jobname,
        "--runner", env_config['env']['v_runner'],
        "--network", "hca-hin-networks-shared-vpc",
        "--subnetwork", env_config['env']["v_subnetwork"],
        "--staging_location",  env_config['env']["v_dfstagebucket"],
        "--temp_location", env_config['env']["v_gcs_temp_bucket"],
        "--region", env_config['env']["v_region"],
        "--save_main_session",
        "--num_workers", "1",
        "--max_num_workers", "2",
        "--no_use_public_ips",
        "--dataflow_service_options, enable_prime",
        "--autoscaling_algorithm", "THROUGHPUT_BASED",
        "--worker_machine_type", "n1-standard-4",
        "--setup_file", '{}/{}/setup.py'.format(utilpath,"data_validation")        
    ]


    #pipeline_options = PipelineOptions(pipeline_args)


    with beam.Pipeline(argv=pipeline_args) as p:
        datasets = p | "Identify Datasets to validate" >> beam.Create(__identify_datasets(dv_config))
        table_config = (
            datasets
            | "Identify Tables to validate" >> (beam.ParDo(IdentifyTablesDoFn())) 
            | "Explore Table Definition" >> (beam.Map(ExploreTableDefinitionDoFn.process))
        )

        count_tables = table_config | "Validate count" >> (beam.Map(ValidateCountDoFn.process)).with_outputs('count_mismatch', main='matched')
        row_tables = table_config | "Validate rows" >> (beam.Map(ValidateRowsDoFn.process)).with_outputs('rows_mismatch', main='matched')
        # column_tables = count_tables.matched | 'Validate columns' >> (beam.Map(ValidateColumnsDoFn.process)).with_outputs('columns_mismatch', main='matched')
        # column_tables.columns_mismatch | 'Columns validation failed' >> (beam.Map(ReportFailedTableDoFn.process))
        # row_tables = count_tables.matched | "Validate rows" >> (beam.Map(ValidateRowsDoFn.process)).with_outputs('rows_mismatch', main='matched')
        count_tables.count_mismatch | "Count validation failed" >> (beam.Map(ReportFailedTableDoFn.process))
        row_tables.rows_mismatch | "Rows validation failed" >> (beam.Map(ReportFailedTableDoFn.process))

if __name__ == '__main__':

    cwd = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(cwd)
    sys.path.insert(0, base_dir)
    utilpath = base_dir + '/utils/'
    config_folder = base_dir + "/"

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--env_config_yaml', required=True, dest='env_config_yaml', help=("Required Environment Config file name"))
    parser.add_argument('--dv_config_yaml', required=True, dest='dv_config_yaml', help=("Required Data Validation Config file name"))

    args = parser.parse_args()

    env_config = __load_config(args.env_config_yaml)
    dv_config = __load_config(args.dv_config_yaml)

    dv_filename = args.dv_config_yaml
    
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d%H%M%S")    
    global jobname
    jobname =  "data-validation-" + dv_filename[dv_filename.rfind("_") + 1:dv_filename.rfind(".")]   + '-' + now_str
    logging.info("===Dataflow Job Name is {} ===".format(jobname))
    run()


