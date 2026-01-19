DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_ar_transaction_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Sean Wilson
      Name: Ref_CC_AR_Transaction_Code - BTEQ Script.
      Mod1: Creation of script on 9/7/2011. SW.
*********************************************************/ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ar_transaction_code_mv (company_code, coid, ar_transaction_id, unit_num, procedure_code, ar_transaction_type, ar_transaction_desc, ar_transaction_category_id, create_login_userid, create_date_time, update_login_userid, update_date_time, inactive_date, dw_last_update_date_time, source_system_code)
SELECT ros.company_code,
       CASE
           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
           ELSE og.client_id
       END AS coido,
       mtm.id AS ar_transaction_id,
       og.short_name AS unit_num,
       mtm.code AS procedure_code,
       mtm.trans_type AS ar_transaction_type,
       mtm.description AS ar_transaction_desc,
       apt.transaction_category_id AS ar_transaction_category_id,
       usr.login_id AS create_login_userid,
       CAST(mtm.date_created AS DATETIME) AS create_date_time,
       usr2.login_id AS update_login_userid,
       CAST(mtm.date_updated AS DATETIME) AS update_date_time,
       mtm.effective_end_date AS inactive_date,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
       'N' AS source_system_code
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_transaction_master AS mtm
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON mtm.schema_id = og.schema_id
AND mtm.org_id = og.org_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON mtm.schema_id = sm.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.apl_transaction AS apt ON mtm.schema_id = apt.schema_id
AND mtm.id = apt.mon_transaction_master_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mtm.user_id_created_by = usr.user_id
AND mtm.schema_id = usr.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON mtm.user_id_modified_by = usr2.user_id
AND mtm.schema_id = usr2.schema_id
INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON mtm.schema_id = ros.schema_id
AND og.org_id = ros.org_id;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;