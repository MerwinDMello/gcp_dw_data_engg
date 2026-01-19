DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/sscdenialscleanup.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/SSCDenialsCleanup.log;
 BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.ssc_denials
WHERE (ssc_denials.schema_id,
       ssc_denials.ssc_id,
       upper(ssc_denials.coid),
       ssc_denials.mon_account_payer_id) IN
    (SELECT AS STRUCT ssc_denials_0.schema_id,
                      ssc_denials_0.ssc_id,
                      upper(max(ssc_denials_0.coid)) AS coid,
                      ssc_denials_0.mon_account_payer_id
     FROM {{ params.param_parallon_ra_stage_dataset_name }}.ssc_denials AS ssc_denials_0
     GROUP BY 1,
              2,
              upper(ssc_denials_0.coid),
              4
     HAVING count(*) > 1)
  AND CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ssc_denials.pass_type) AS FLOAT64) = 2
  AND ssc_denials.schema_id = 3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;