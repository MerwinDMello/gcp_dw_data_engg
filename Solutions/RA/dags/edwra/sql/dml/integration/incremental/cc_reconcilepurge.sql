DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reconcilepurge.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/Logs/CC_ReconcilePurge.log;
 BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reconcile
WHERE DATE(cc_reconcile.created_date_time) <= CAST(trim(concat(substr(format_date('%Y-%m-%d', date_add(current_date('US/Central'), interval -6 MONTH)), 1, 7), '-01')) AS DATE);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;