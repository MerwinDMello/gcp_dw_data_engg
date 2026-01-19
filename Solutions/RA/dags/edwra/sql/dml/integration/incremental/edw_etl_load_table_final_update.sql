DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/edw_etl_load_table_final_update.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************************************************************************************
 Developer: Brian Ciampa
      Name: ETL_LOAD_TABLE_FINAL_UPDATE - BTEQ Script.
      Mod1: Creation of script on 8/30/2018. BC.
********************************************************************************************************************************************************/ -- SET QUERY_BAND = 'App=RA_Group2_ETL;' FOR SESSION;
 BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.etl_load AS a
SET etl_load_end_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    is_current = 0,
    last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
FROM
  (SELECT b.etl_load_date,
          b.etl_load_begin_time,
          b.etl_load_end_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.etl_load AS b
   WHERE b.is_current = 1 ) AS c
WHERE a.etl_load_date = c.etl_load_date
  AND a.etl_load_begin_time = c.etl_load_begin_time
  AND a.etl_load_end_time IS NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.etl_load
WHERE DATE(etl_load.last_update_date_time) < date_sub(current_date('US/Central'), interval 400 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;