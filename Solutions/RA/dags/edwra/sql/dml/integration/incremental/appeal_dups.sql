-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/appeal_dups.sql
-- Translated from: bteq
-- Translated to: BigQuery
 -- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Appeal_Dups.out;
 BEGIN
SELECT count(*)
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
GROUP BY upper(cc_appeal.company_code),
         upper(cc_appeal.coid),
         cc_appeal.patient_dw_id,
         cc_appeal.payor_dw_id,
         cc_appeal.iplan_insurance_order_num,
         cc_appeal.appeal_num
HAVING count(*) > 1;


EXCEPTION WHEN ERROR THEN BEGIN /* Empty. */ END;

END;

-- .IF ACTIVITYCOUNT > 0 THEN .GOTO REMOVE_DUPS
-- .GOTO EXIT_SCRIPT
BEGIN
DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS a
WHERE (upper(a.company_code),
       upper(a.coid),
       a.patient_dw_id,
       a.payor_dw_id,
       a.iplan_insurance_order_num,
       a.appeal_num) IN
    (SELECT AS STRUCT upper(max(b.company_code)) AS company_code,
                      upper(max(b.coid)) AS coid,
                      b.patient_dw_id,
                      b.payor_dw_id,
                      b.iplan_insurance_order_num,
                      b.appeal_num
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS b
     GROUP BY upper(b.company_code),
              upper(b.coid),
              3,
              4,
              5,
              6
     HAVING count(*) > 1);


EXCEPTION WHEN ERROR THEN BEGIN /* Empty. */ END;

END;

-- .GOTO EXIT_SCRIPT;