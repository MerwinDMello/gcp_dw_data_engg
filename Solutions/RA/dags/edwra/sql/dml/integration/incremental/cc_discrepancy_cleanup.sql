DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_discrepancy_cleanup.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***********************************************************
    Developer: Sean Wilson
         Name: CC_Discrepancy_Cleanup.sql - BTEQ Script.
Creation Date: 10/8/2014
         Mod1: PBI13619- Change archive from 40 days to 90
		       days.  SW 10/10/2017.
		 Mod2: PBI15017- Change archive from 90 days to 400
               days. SW 1/10/2018.
************************************************************/ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_discrepancy
WHERE date_sub(current_date('US/Central'), interval 2555 DAY) > DATE(cc_discrepancy.extract_date_time)
  AND extract(DAY
              FROM cc_discrepancy.extract_date_time) > 6;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_discrepancy
WHERE DATE(cc_discrepancy.extract_date_time) < date_add(current_date('US/Central'), interval -7 YEAR);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;