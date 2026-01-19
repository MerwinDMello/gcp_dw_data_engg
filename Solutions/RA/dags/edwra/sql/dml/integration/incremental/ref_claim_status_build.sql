DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_claim_status_build.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**************************************************************
   Developer: Sean Wilson
        Date: 2/18/2011
        Name: Ref_Claim_Status_Build.sql
        Mod1: Initial creation of BTEQ script on 2/1/2011.
        Mod2: Added Status_Phase on 04/04/2011 (HR)
***************************************************************/ BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_claim_status AS x USING
  (SELECT mon_status.id AS claim_status_code,
          mon_status.name AS claim_status_name,
          mon_status.description AS claim_status_desc,
          mon_status_category.id AS claim_status_category_code,
          mon_status.status_phase AS claim_status_phase
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_status
   CROSS JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_status_category
   WHERE mon_status.mon_stat_category_id = mon_status_category.id ) AS z ON x.claim_status_code = z.claim_status_code WHEN MATCHED THEN
UPDATE
SET claim_status_category_code = z.claim_status_category_code,
    claim_status_name = z.claim_status_name,
    claim_status_desc = z.claim_status_desc,
    status_phase = z.claim_status_phase,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_status_code,
        claim_status_category_code,
        claim_status_name,
        claim_status_desc,
        status_phase,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.claim_status_code, z.claim_status_category_code, z.claim_status_name, z.claim_status_desc, z.claim_status_phase, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;