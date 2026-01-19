DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reimbursement_discrepancy_chks.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ROW_COUNT INT64;

DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA234_Reim_Disc_Chks;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


SET _ROW_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cc_reimbursement_discrepancy.*
      FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy
      LIMIT 5) AS subselect);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

IF _ROW_COUNT = 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', 20, '. Error context: ', _ERROR_MSG);

END IF;
END;
--  AR Bill Thru Date Chk
BEGIN
SET _ERROR_CODE = 0;


SET _ROW_COUNT =
  (SELECT count(*)
   FROM
     (SELECT max(cc_reimbursement_discrepancy.coid) AS coid,
             count(*)
      FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy
      WHERE cc_reimbursement_discrepancy.ar_bill_thru_date = DATE '1800-01-01'
      GROUP BY upper(cc_reimbursement_discrepancy.coid)) AS subselect);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

IF _ROW_COUNT > 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', 20, '. Error context: ', _ERROR_MSG);

END IF;

RETURN;

RETURN;