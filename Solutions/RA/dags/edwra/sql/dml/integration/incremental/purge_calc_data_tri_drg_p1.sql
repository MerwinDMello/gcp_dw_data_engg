DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/purge_calc_data_tri_drg.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/$1_Purge.out;
 BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `{{ params.param_parallon_ra_stage_dataset_name }}`.`$3`
SELECT *
FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.`$1` AS a
WHERE NOT EXISTS
    (SELECT 1
     FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.mon_account_payer_calc_latest AS b
     WHERE a.`$2` = b.id
       AND a.schema_id = b.schema_id )
  AND a.schema_id = a.`$schemaid`;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.`$1` AS a
WHERE NOT EXISTS
    (SELECT 1
     FROM `{{ params.param_parallon_ra_stage_dataset_name }}`.mon_account_payer_calc_latest AS b
     WHERE a.`$2` = b.id
       AND a.schema_id = b.schema_id )
  AND a.schema_id = a.`$schemaid`;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;