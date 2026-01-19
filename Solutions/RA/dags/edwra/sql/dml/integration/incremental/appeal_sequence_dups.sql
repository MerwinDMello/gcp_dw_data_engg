-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/appeal_sequence_dups.sql
-- Translated from: bteq
-- Translated to: BigQuery
 -- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Appeal_Sequence_Dups.out;
 BEGIN
SELECT count(*)
FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
GROUP BY upper(cc_appeal_sequence.company_code),
         upper(cc_appeal_sequence.coid),
         cc_appeal_sequence.patient_dw_id,
         cc_appeal_sequence.payor_dw_id,
         cc_appeal_sequence.iplan_insurance_order_num,
         cc_appeal_sequence.appeal_num,
         cc_appeal_sequence.appeal_seq_num
HAVING count(*) > 1;


EXCEPTION WHEN ERROR THEN BEGIN /* Empty. */ END;

END;

-- .IF ACTIVITYCOUNT > 0 THEN .GOTO REMOVE_DUPS
-- .GOTO EXIT_SCRIPT
BEGIN
DELETE
FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence AS a
WHERE (upper(a.company_code),
       upper(a.coid),
       a.patient_dw_id,
       a.payor_dw_id,
       a.iplan_insurance_order_num,
       a.appeal_num,
       a.appeal_seq_num) IN
    (SELECT AS STRUCT upper(max(b.company_code)) AS company_code,
                      upper(max(b.coid)) AS coid,
                      b.patient_dw_id,
                      b.payor_dw_id,
                      b.iplan_insurance_order_num,
                      b.appeal_num,
                      b.appeal_seq_num
     FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence AS b
     GROUP BY upper(b.company_code),
              upper(b.coid),
              3,
              4,
              5,
              6,
              7
     HAVING count(*) > 1);


EXCEPTION WHEN ERROR THEN BEGIN /* Empty. */ END;

END;

-- .GOTO EXIT_SCRIPT;