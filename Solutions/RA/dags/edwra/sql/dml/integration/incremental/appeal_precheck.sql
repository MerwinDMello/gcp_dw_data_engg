DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/appeal_precheck.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ROW_COUNT INT64;

DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

BEGIN
SET _ERROR_CODE = 0;


SET _ROW_COUNT =
  (SELECT count(*)
   FROM
     (SELECT max(cc_appeal.company_code) AS company_code,
             max(cc_appeal.coid) AS coid,
             cc_appeal.patient_dw_id,
             cc_appeal.payor_dw_id,
             cc_appeal.iplan_insurance_order_num,
             cc_appeal.appeal_num,
             count(*) AS ROW_COUNT
      FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
      GROUP BY upper(cc_appeal.company_code),
               upper(cc_appeal.coid),
               3,
               4,
               5,
               6
      HAVING count(*) > 1) AS subselect);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

IF _ROW_COUNT > 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', 20, '. Error context: ', _ERROR_MSG);

END IF;