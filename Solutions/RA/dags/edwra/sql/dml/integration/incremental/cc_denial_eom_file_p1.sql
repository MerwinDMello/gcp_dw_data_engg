DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_denial_eom_file.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Denial_EOM_File.out;
 BEGIN
SET _ERROR_CODE = 0;

EXPORT DATA OPTIONS(
  uri= "{{ params.param_gcs_outbound_bucket }}CC_DenialEDW_Export*.csv",
  format='CSV',
  overwrite=true,
  --header=true,
  field_delimiter=',') AS
SELECT concat('"', format_date('%m/%d/%Y', cc_denial_eom.report_end_date), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.denial_status_code), ''), '"'), CONCAT('"', trim(cc_denial_eom.unit_num), '"'), rtrim(trim(format('%#14.0f', cc_denial_eom.pat_acct_num)), '.'), trim(format('%11d', cc_denial_eom.iplan_id)), trim(format('%11d', cc_denial_eom.iplan_insurance_order_num)), CONCAT('"', trim(cc_denial_eom.patient_type_code), '"'), trim(format('%11d', cc_denial_eom.patient_financial_class_code)), trim(format('%11d', cc_denial_eom.payor_financial_class_code)), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.appeal_origination_date), 1, 10), ''), '"'), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.appeal_level_origination_date), 1, 10), ''), '"'), coalesce(trim(format('%11d', cc_denial_eom.disposition_num)), ''), coalesce(trim(format('%11d', cc_denial_eom.appeal_level_num)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.beginning_balance_amt)), ''), trim(format('%11d', cc_denial_eom.beginning_balance_cnt)), coalesce(trim(format('%#20.3f', cc_denial_eom.beginning_appeal_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.new_denial_account_amt)), ''), trim(format('%11d', cc_denial_eom.new_denial_account_cnt)), trim(format('%#20.3f', cc_denial_eom.unworked_conversion_amt)), trim(format('%11d', cc_denial_eom.unworked_new_accounts_cnt)), coalesce(trim(format('%#20.3f', cc_denial_eom.not_true_denial_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.write_off_denial_account_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.overturned_account_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.corrections_account_amt)), ''), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.appeal_closing_date), 1, 10), ''), '"'), coalesce(trim(format('%#20.3f', cc_denial_eom.trans_next_party_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.ending_balance_amt)), ''), trim(format('%11d', cc_denial_eom.resolved_accounts_cnt)), coalesce(trim(format('%#20.3f', cc_denial_eom.total_charge_amt)), ''), CONCAT('"', coalesce(trim(cc_denial_eom.attending_physician_name_id), ''), ')"'), coalesce(trim(format('%#20.3f', cc_denial_eom.account_balance_amt)), ''), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.discharge_date), 1, 10), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.service_code), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.medical_record_num), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.last_update_hca_3_4_id), ''), '"'), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.last_update_date), 1, 10), ''), '"'), CONCAT('"',coalesce(trim(CAST(cc_denial_eom.work_again_date AS STRING)), ''),'"'), CONCAT('"', coalesce(substr(format_date('%m/%d/%Y', cc_denial_eom.appeal_deadline_date), 1, 10), ''), '"'), trim(format('%#20.3f', cc_denial_eom.denied_charges_amt)), coalesce(trim(format('%#20.3f', cc_denial_eom.cash_adjustment_amt)), ''), coalesce(trim(format('%#20.3f', cc_denial_eom.ca_adjustment_amt)), ''), CONCAT('"', coalesce(trim(cc_denial_eom.root_cause), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.root_cause_desc), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.denial_code_category), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.disposition_code), ''), '"'), trim(format('%11d', cc_denial_eom.sequence_number)), CONCAT('"', coalesce(trim(cc_denial_eom.appeal_code), ''), '"'), CONCAT('"', coalesce(trim(cc_denial_eom.appeal_code_desc), ''), '"')
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom
WHERE cc_denial_eom.report_end_date =
    (SELECT max(cc_denial_eom_0.report_end_date)
     FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom AS cc_denial_eom_0
     WHERE cc_denial_eom_0.schema_id = 1 )
  AND CASE
          WHEN cc_denial_eom.schema_id = 3 THEN 1
          ELSE cc_denial_eom.schema_id
      END = 1
ORDER BY cc_denial_eom.pat_acct_num,
         cc_denial_eom.iplan_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;