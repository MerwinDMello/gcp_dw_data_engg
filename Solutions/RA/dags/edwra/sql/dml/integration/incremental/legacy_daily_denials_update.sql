DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/legacy_daily_denials_update.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Legacy_Daily_Denials_Update.out;
 BEGIN
SET _ERROR_CODE = 0;


UPDATE  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials
SET close_date = legacy_daily_denials.appeal_date_created
WHERE legacy_daily_denials.close_date IS NULL
  AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(legacy_daily_denials.disp_code) AS FLOAT64) IN
    (SELECT CAST(disposition.dispcode AS FLOAT64) AS dispcode
     FROM  {{ params.param_parallon_ra_stage_dataset_name }}.disposition
     WHERE upper(rtrim(disposition.disptype)) IN('C',
                                                 'I') );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


UPDATE  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials
SET close_date =
  (SELECT min(b.appeal_date_created)
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials AS b
   WHERE upper(rtrim(b.unit_num)) = upper(rtrim(b.unit_num))
     AND upper(rtrim(b.account_no)) = upper(rtrim(b.account_no))
     AND b.iplan_id = b.iplan_id
     AND b.payer_rank = b.payer_rank )
WHERE legacy_daily_denials.appeal_date_created IS NULL
  AND legacy_daily_denials.close_date IS NULL
  AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(legacy_daily_denials.disp_code) AS FLOAT64) IN
    (SELECT CAST(disposition.dispcode AS FLOAT64) AS dispcode
     FROM  {{ params.param_parallon_ra_stage_dataset_name }}.disposition
     WHERE upper(rtrim(disposition.disptype)) IN('C',
                                                 'I') );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


UPDATE  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials
SET close_date = CAST(NULL AS DATE)
WHERE legacy_daily_denials.close_date IS NOT NULL
  AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(legacy_daily_denials.disp_code) AS FLOAT64) IN
    (SELECT CAST(disposition.dispcode AS FLOAT64) AS dispcode
     FROM  {{ params.param_parallon_ra_stage_dataset_name }}.disposition
     WHERE upper(rtrim(disposition.disptype)) IN('O') );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.legacy_daily_denials
WHERE legacy_daily_denials.close_date IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;