CREATE OR REPLACE PROCEDURE `{{ params.param_hr_core_dataset_name }}`.sk_gen (dbname STRING, tablename STRING, columnname STRING, sk_type STRING)
BEGIN 
       DECLARE cnt INT64;
       DECLARE max_sk NUMERIC(29);
       DECLARE sql_query STRING;
       SET sql_query = (SELECT FORMAT( '''
              SELECT COUNT(*) 
              FROM {{ params.param_hr_stage_dataset_name }}.ref_sk_type 
              WHERE sk_type = '%s';''',sk_type ));
       
       EXECUTE IMMEDIATE sql_query INTO cnt;

       IF cnt = 0 THEN
              SET max_sk = ( SELECT COALESCE(MAX(sk_code),0)+1 FROM {{ params.param_hr_stage_dataset_name }}.ref_sk_type);
              INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_sk_type
              VALUES ( cnt, sk_type, datetime_trunc(current_datetime('US/Central'), SECOND),columnname); 
       END IF;

       SET sql_query = (SELECT FORMAT( '''
              SELECT COALESCE(MAX(sk),0)
              FROM {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk 
              WHERE sk_type='%s';''',sk_type ));
       
       EXECUTE IMMEDIATE sql_query INTO max_sk;

       SET sql_query = (SELECT FORMAT( '''
              INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk
              SELECT 
              (%t + row_number() over(order by new_sk.sk_source_txt)) as sk,
              new_sk.sk_source_txt,
              new_sk.sk_type,
              datetime_trunc(current_datetime('US/Central'), SECOND) from 
              (SELECT distinct 
                     %s as sk_source_txt, 
                     '%s' as sk_type 
              from `%s.%s`
              WHERE %s NOT IN (
                     SELECT sk_source_txt from {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk 
                     WHERE sk_type='%s')) new_sk
              INNER JOIN
              {{ params.param_hr_stage_dataset_name }}.ref_sk_type st
              on st.sk_type = new_sk.sk_type; 
                     ''',max_sk, columnname, sk_type,dbname, tablename,columnname, sk_type));
       EXECUTE IMMEDIATE sql_query;
END;