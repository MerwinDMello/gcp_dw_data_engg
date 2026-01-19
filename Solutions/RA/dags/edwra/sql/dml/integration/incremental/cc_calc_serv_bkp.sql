DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_calc_serv_bkp.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***********************************************************************************************************************************************************************************
   Developer : Saravana Moorthy
        Date: 9/12/2023
        Name: CC_CALC_SERV_BKP.sql
***********************************************************************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA140;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DROP TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service_bkp;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


CREATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service_bkp COPY  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;