DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_edw_remit_recon_aggregated_del.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- ************************************************************************** Developer: Tawale Pritam Name: CC_EDW_Remit_Recon_Aggregated_Del - BTEQ Script. Date: Creation OF script ON 06/21/2016 ****************************************************************************/
 -- bteq << EOF > /etl/ST/CT/CC_EDW/TgtFiles/CC_EDW_Remit_Recon_Aggregated_P1.out;
 BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_recon_aggregated
WHERE DATE(cc_remit_recon_aggregated.dw_last_update_date_time) = date_sub(current_date('US/Central'), interval 54 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.edw_remit_recon_aggregated
WHERE DATE(edw_remit_recon_aggregated.dw_last_update_date_time) = date_sub(current_date('US/Central'), interval 54 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;