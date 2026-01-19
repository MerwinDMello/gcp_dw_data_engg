CREATE OR REPLACE PROCEDURE `{{ params.param_hr_core_dataset_name }}.copy_table_data_with_valid_to_date_adjustment`(project_name STRING, ds_name STRING, tbl_name STRING, last_update_date DATE, up_date STRING, use_copy_dataset BOOLEAN, target_copy_dataset BOOLEAN)
BEGIN

DECLARE  select_query, insert_Query, final_Query,sql_query  string;

DECLARE cnt int64;
DECLARE copy_dataset string;
DECLARE copy_dataset_target string;
DECLARE valid_to_date_cnt int64;
DECLARE update_column string;

EXECUTE IMMEDIATE CONCAT("Select If("||use_copy_dataset||",'_copy','')") INTO copy_dataset;

IF project_name = "hca-hin-dev-cur-hr" THEN
  SET target_copy_dataset = TRUE;
END IF;

EXECUTE IMMEDIATE CONCAT("Select If("||target_copy_dataset||",'_copy','')") INTO copy_dataset_target;

EXECUTE IMMEDIATE CONCAT("Select Count(table_id) from `hca-hin-dev-cur-hr`."||ds_name||copy_dataset||".__TABLES__ Where table_id = '"||tbl_name||"'") INTO cnt;

IF cnt = 0 THEN
  RAISE USING MESSAGE = CONCAT("Source Table ",tbl_name," not found in Dataset ",ds_name, copy_dataset);
END IF;

EXECUTE IMMEDIATE CONCAT("Select Count(table_id) from "||ds_name||copy_dataset_target||".__TABLES__ Where table_id = '"||tbl_name||"'") INTO cnt;

IF cnt = 0 THEN
  RAISE USING MESSAGE = CONCAT("Destination Table ",tbl_name," not found in Dataset ",ds_name,copy_dataset_target);
END IF;

EXECUTE IMMEDIATE CONCAT( "TRUNCATE TABLE ", ds_name, copy_dataset_target, ".", tbl_name);

EXECUTE IMMEDIATE CONCAT( "SELECT STRING_AGG((CASE WHEN Lower(B.column_name) = 'valid_to_date' AND NOT " || use_copy_dataset || " THEN concat ( 'DATETIME(', IF(Lower(B.column_name) = Lower(B.table_name), B.table_name || '.' , '') || B.column_name, ',',CHR(39),'23:59:59',CHR(39), ')') WHEN (A.data_type = B.data_type OR (B.data_type = 'NUMERIC' AND A.data_type <> 'INT64') OR (B.data_type = 'BIGNUMERIC' AND A.data_type <> 'INT64'))  THEN IF(Lower(B.column_name) = Lower(B.table_name), B.table_name || '.' , '') || B.column_name ELSE concat ( 'CAST (',IF(Lower(B.column_name) = Lower(B.table_name), B.table_name || '.' , '') || B.column_name,' AS ',A.data_type,')') END ),', ' order by A.ordinal_position) FROM "||ds_name||copy_dataset_target||".INFORMATION_SCHEMA.COLUMNS A INNER JOIN `hca-hin-dev-cur-hr`."||ds_name||copy_dataset||".INFORMATION_SCHEMA.COLUMNS B ON A.table_name = B.table_name AND A.column_name = B.column_name WHERE A.table_name = '"||tbl_name||"'") INTO select_query;

Select select_query;

EXECUTE IMMEDIATE CONCAT( " SELECT STRING_AGG((A.column_name),' , ' order by A.ordinal_position) FROM "||ds_name||copy_dataset_target||".INFORMATION_SCHEMA.COLUMNS A INNER JOIN `hca-hin-dev-cur-hr`."||ds_name||copy_dataset||".INFORMATION_SCHEMA.COLUMNS B ON A.table_name = B.table_name AND A.column_name = B.column_name WHERE A.table_name = '"||tbl_name||"'") INTO insert_Query;

SELECT insert_Query;

SET final_Query = CONCAT(" INSERT INTO ", ds_name, copy_dataset_target, ".", tbl_name," (", insert_Query,") Select ",select_query," From `hca-hin-dev-cur-hr`.", ds_name,copy_dataset, ".", tbl_name);

Select final_Query;

EXECUTE IMMEDIATE final_Query;

IF up_date = 'TRUE' THEN

  -- Check if Table has SCD2 Logic by checking if table has Valid To Date
  EXECUTE IMMEDIATE CONCAT("SELECT Count(*) as rec_count FROM "||ds_name||copy_dataset||".INFORMATION_SCHEMA.COLUMNS Where table_name = '"||tbl_name||"' AND column_name = 'valid_to_date'") INTO valid_to_date_cnt;

  -- Check which field is used for Last Update Timestamp
  EXECUTE IMMEDIATE CONCAT("SELECT column_name FROM "||ds_name||copy_dataset||".INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"||tbl_name||"' AND column_name IN ('dw_last_update_date_time', 'dw_last_update_date_tim', 'sk_generated_date_time', 'dw_last_update_time')") INTO update_column;

  IF valid_to_date_cnt > 0 THEN
    -- Delete inserted records 
    SET sql_query = (SELECT FORMAT( '''
                  DELETE FROM `%s.%s%s.%s`
                  WHERE DATE(%s) = '%t'
                  AND VALID_TO_DATE = DATETIME('9999-12-31 23:59:59');
                  ''',project_name, ds_name, copy_dataset_target, tbl_name, update_column, last_update_date));
          
    EXECUTE IMMEDIATE sql_query;
    
    -- Update changed records valid_to_date to 9999-12-31
    SET sql_query = (SELECT FORMAT( '''
                  UPDATE `%s.%s%s.%s`
                  SET VALID_TO_DATE = DATETIME('9999-12-31 23:59:59')
                  WHERE DATE(%s) = '%t'
                  AND DATE(VALID_TO_DATE) <> '9999-12-31';
                  ''',project_name, ds_name, copy_dataset_target, tbl_name, update_column, last_update_date));
          
    EXECUTE IMMEDIATE sql_query;
  ELSE
  -- Remove Records from reference tables which use SCD1 Logic
    SET sql_query = (SELECT FORMAT( '''
                  DELETE FROM `%s.%s%s.%s`
                  WHERE DATE(%s) >= '%t'
                  ''',project_name, ds_name, copy_dataset_target, tbl_name, update_column, last_update_date));
          
    EXECUTE IMMEDIATE sql_query;
  END IF;
END IF;
END;