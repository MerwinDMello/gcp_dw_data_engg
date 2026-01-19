DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_customer_build.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*****************************************************************
      Developer: Holly Ray
           Date: 6/22/2011
           Name: Ref_CC_Customer_Build.sql
           Mod1: Initial creation of BTEQ script on 6/22/2011.
******************************************************************/ BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_customer AS x USING
  (SELECT c.org_id AS cc_customer_id,
          c.name AS cc_customer_name,
          NULL AS cc_schema_id
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.customer AS c) AS z ON x.cc_customer_id = z.cc_customer_id WHEN MATCHED THEN
UPDATE
SET cc_customer_name = z.cc_customer_name,
    cc_schema_id = z.cc_schema_id,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (cc_customer_id,
        cc_customer_name,
        cc_schema_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.cc_customer_id, z.cc_customer_name, z.cc_schema_id, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;