CREATE OR REPLACE PROCEDURE `{{ params.param_hr_core_dataset_name }}.sk_gen`(dbname STRING, tablename STRING, columnname STRING, sk_type STRING)
BEGIN 
       DECLARE cnt INT64;
       DECLARE max_sk NUMERIC(29);
       DECLARE sql_query STRING;
       SET sql_query = (SELECT FORMAT( '''
              SELECT COUNT(*) 
              FROM `%s.ref_sk_type` 
              WHERE upper(sk_type) = upper('%s');''',dbname,sk_type ));
       
       EXECUTE IMMEDIATE sql_query INTO cnt;

       IF cnt = 0 THEN
              SET sql_query = (SELECT FORMAT( '''
              SELECT COALESCE(MAX(sk_code),0)+1
              FROM `%s.ref_sk_type`;''',dbname ));
       
              EXECUTE IMMEDIATE sql_query INTO max_sk;
              
              SET sql_query = (SELECT FORMAT( '''
              INSERT INTO `%s.ref_sk_type`
              VALUES ( cnt, sk_type, datetime_trunc(current_datetime('US/Central'), SECOND),columnname);''',dbname ));

              EXECUTE IMMEDIATE sql_query;

       END IF;

       SET sql_query = (SELECT FORMAT( '''
              SELECT COALESCE(MAX(sk),0)
              FROM `%s.ref_sk_xwlk` 
              WHERE upper(sk_type)=upper('%s');''',dbname,sk_type ));
       
       EXECUTE IMMEDIATE sql_query INTO max_sk;

       SET sql_query = (SELECT FORMAT( '''
              INSERT INTO `%s.ref_sk_xwlk`
              SELECT 
              (%t + row_number() over(order by new_sk.sk_source_txt)) as sk,
              new_sk.sk_source_txt,
              new_sk.sk_type,
              datetime_trunc(current_datetime('US/Central'), SECOND) from 
              (SELECT distinct 
                     %s as sk_source_txt, 
                     '%s' as sk_type 
              from `%s.%s`
              WHERE upper(%s) NOT IN (
                     SELECT upper(sk_source_txt) from `%s.ref_sk_xwlk` 
                     WHERE upper(sk_type)=upper('%s'))
              QUALIFY row_number() OVER (PARTITION BY upper(%s), upper('%s')
               ORDER BY upper(%s), upper('%s') DESC) = 1) new_sk
       
              INNER JOIN
              `%s.ref_sk_type` st
              on upper(st.sk_type) = upper(new_sk.sk_type)
              QUALIFY row_number() OVER (PARTITION BY new_sk.sk_source_txt, new_sk.sk_type
               ORDER BY new_sk.sk_source_txt, new_sk.sk_type DESC) = 1; 
                     ''',dbname, max_sk, columnname, sk_type,dbname, tablename,columnname, dbname,sk_type,columnname,sk_type,columnname,sk_type,dbname));
       EXECUTE IMMEDIATE sql_query;
END;