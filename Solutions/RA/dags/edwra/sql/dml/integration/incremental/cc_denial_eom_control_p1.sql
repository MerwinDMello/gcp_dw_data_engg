DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_denial_eom_control.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Denial_EOM_Control.out;
 BEGIN
SET _ERROR_CODE = 0;

EXPORT DATA OPTIONS(
  uri= "{{ params.param_gcs_outbound_bucket }}CC_DenialEDW_File_Export_Control*.csv",
  format='CSV',
  overwrite=true,
  header=false,
  field_delimiter=',') AS
SELECT * EXCEPT(order_key) FROM (
SELECT '"MonthEnding"','"BegBalance_Sum"','"BegAccts_Sum"','"EndingBalance_Sum"','"ResolvedAccts_Sum"','"PatNum_Cnt"', 1 as order_key
UNION ALL
SELECT CONCAT('"', trim(format_date('%m/%d/%Y', cc_denial_eom.report_end_date)), '"'), trim(CAST(sum(coalesce(cc_denial_eom.beginning_balance_amt, NUMERIC '0')) AS STRING)),
       trim(format('%11d', sum(coalesce(cc_denial_eom.beginning_balance_cnt, 0)))), trim(CAST(sum(coalesce(cc_denial_eom.ending_balance_amt, NUMERIC '0')) AS STRING)),
       trim(format('%11d', sum(coalesce(cc_denial_eom.resolved_accounts_cnt, 0)))), trim(format('%20d', count(cc_denial_eom.pat_acct_num))), 2 as order_key
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom
WHERE cc_denial_eom.report_end_date =
    (SELECT max(cc_denial_eom_0.report_end_date)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom AS cc_denial_eom_0
     WHERE cc_denial_eom_0.schema_id = 1 )
  AND CASE
          WHEN cc_denial_eom.schema_id = 3 THEN 1
          ELSE cc_denial_eom.schema_id
      END = 1
GROUP BY cc_denial_eom.report_end_date)
ORDER BY order_key;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;