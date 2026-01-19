DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/del_mon_account_nonclinical_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Saravana Moorthy
  Purpose - Purge contents in edwra_Staging.mon_Account_non_clinical_Code to sync with Concuity ( Purge)
 mod1 : Initial Version
*********************************************************/ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_nonclinical_code_c AS a
WHERE EXISTS
    (SELECT 1
     FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_nonclinical_code_del AS b
     WHERE a.id = b.id
       AND a.schema_id = b.schema_id );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;