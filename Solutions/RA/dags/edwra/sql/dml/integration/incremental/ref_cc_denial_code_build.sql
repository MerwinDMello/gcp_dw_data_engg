DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_denial_code_build.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************************
  Developer: Sean Wilson
       Date: 3/31/2011
       Name: Ref_CC_Denial_Code_Build.sql
       Mod1: Initial creation of BTEQ script on 3/31/2011.
****************************************************************************************/ BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_denial_code AS x USING
  (SELECT apl.id AS cc_denial_code_id,
          apl.code AS denial_code,
          apl.description AS denial_description,
          apl.denial_category_id AS denial_category_id,
          apl.external_code AS pa_denial_code,
          apl.is_user_assignable AS user_assignable_sw
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apl_appeal AS apl
   WHERE apl.external_code IS NOT NULL ) AS z ON x.cc_denial_code_id = z.cc_denial_code_id WHEN MATCHED THEN
UPDATE
SET denial_code = z.denial_code,
    denial_description = z.denial_description,
    denial_category_id = z.denial_category_id,
    pa_denial_code = z.pa_denial_code,
    user_assignable_sw = z.user_assignable_sw,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (cc_denial_code_id,
        denial_code,
        denial_description,
        denial_category_id,
        pa_denial_code,
        user_assignable_sw,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.cc_denial_code_id, z.denial_code, z.denial_description, z.denial_category_id, z.pa_denial_code, z.user_assignable_sw, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;