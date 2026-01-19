DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/edw_daily_denials_schemaid_update.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/Richmond_SchemaID_Update.log;
 BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
SET schema_id = 1
WHERE upper(rtrim(edw_daily_denial_inventory.ssc_name)) = 'RICHMOND SSC'
  AND edw_daily_denial_inventory.schema_id = 3;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;