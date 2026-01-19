DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/etl_cc_source_row_counts_cleanup.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***********************************************************
    Developer: Brian Ciampa
         Name: ETL_CC_SOURCE_ROW_COUNTS_CLEANUP.sql - BTEQ Script.
Creation Date: 7/19/2018
  Description: Purges all data from EDWRA_STAGING.ETL_CC_SOURCE_ROW_COUNTS that is older than 13 months.
************************************************************/ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.etl_cc_source_row_counts
WHERE DATE(etl_cc_source_row_counts.dw_last_update_date) < date_add(current_date('US/Central'), interval -13 MONTH);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;