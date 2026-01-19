DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_charge.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_charge AS core_charge 
USING {{ params.param_clm_stage_dataset_name }}.fact_charge AS stg_charge
ON upper(trim(core_charge.claim_id, ' ')) = upper(trim(stg_charge.claim_id, ' '))
AND core_charge.charge_seq_num = stg_charge.charge_seq_num
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    charge_revenue_code = TRIM(stg_charge.charge_revenue_code),
    charge_ndc_code = TRIM(stg_charge.charge_ndc_code),
    charge_hcpcs = TRIM(stg_charge.charge_hcpcs),
    charge_rate_value = TRIM(stg_charge.charge_rate_value),
    charge_hcpcs_modifier1_cd = TRIM(stg_charge.charge_hcpcs_modifier1_cd),
    charge_hcpcs_modifier2_cd = TRIM(stg_charge.charge_hcpcs_modifier2_cd),
    charge_hcpcs_modifier3_cd = TRIM(stg_charge.charge_hcpcs_modifier3_cd),
    charge_hcpcs_modifier4_cd = TRIM(stg_charge.charge_hcpcs_modifier4_cd),
    charge_service_dt = stg_charge.charge_service_dt,
    charge_unit_of_svc_num = stg_charge.charge_unit_of_svc_num,
    charge_total_amt = stg_charge.charge_total_amt,
    charge_non_covered_amt = stg_charge.charge_non_covered_amt,
    charge_ndc_drug_qty = stg_charge.charge_ndc_drug_qty,
    charge_ndc_drug_uom = TRIM(stg_charge.charge_ndc_drug_uom)
WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        charge_seq_num,
        charge_revenue_code,
        charge_ndc_code,
        charge_hcpcs,
        charge_rate_value,
        charge_hcpcs_modifier1_cd,
        charge_hcpcs_modifier2_cd,
        charge_hcpcs_modifier3_cd,
        charge_hcpcs_modifier4_cd,
        charge_service_dt,
        charge_unit_of_svc_num,
        charge_total_amt,
        charge_non_covered_amt,
        dw_last_update_date_time,
        source_system_code,
        charge_ndc_drug_qty,
        charge_ndc_drug_uom)
VALUES (TRIM(stg_charge.claim_id), stg_charge.charge_seq_num, TRIM(stg_charge.charge_revenue_code),
TRIM(stg_charge.charge_ndc_code), TRIM(stg_charge.charge_hcpcs), TRIM(stg_charge.charge_rate_value),
TRIM(stg_charge.charge_hcpcs_modifier1_cd), TRIM(stg_charge.charge_hcpcs_modifier2_cd),
TRIM(stg_charge.charge_hcpcs_modifier3_cd), TRIM(stg_charge.charge_hcpcs_modifier4_cd),
stg_charge.charge_service_dt, stg_charge.charge_unit_of_svc_num, stg_charge.charge_total_amt,
stg_charge.charge_non_covered_amt, datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(stg_charge.source_system_code), stg_charge.charge_ndc_drug_qty, TRIM(stg_charge.charge_ndc_drug_uom));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id,
             charge_seq_num
      FROM {{ params.param_clm_core_dataset_name }}.fact_charge
      GROUP BY claim_id,
               charge_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_charge');

ELSE
COMMIT TRANSACTION;

END IF;