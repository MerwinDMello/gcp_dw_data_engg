DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_claim_insurance.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_claim_insurance AS core_claim_insurance
USING {{ params.param_clm_stage_dataset_name }}.fact_claim_insurance AS stg_claim_insurance
ON upper(trim(core_claim_insurance.claim_id, ' ')) = upper(trim(stg_claim_insurance.claim_id, ' '))
AND upper(trim(core_claim_insurance.payor_seq_ind, ' ')) = upper(trim(stg_claim_insurance.payor_seq_ind, ' '))
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    iplan_id = TRIM(stg_claim_insurance.iplan_id),
    payer_id = TRIM(stg_claim_insurance.payer_id),
    payer_sub_id = TRIM(stg_claim_insurance.payer_sub_id),
    payer_name = TRIM(stg_claim_insurance.payer_name),
    health_plan_id = TRIM(stg_claim_insurance.health_plan_id),
    release_info_cert_desc = TRIM(stg_claim_insurance.release_info_cert_desc),
    assign_benefit_cert_desc = TRIM(stg_claim_insurance.assign_benefit_cert_desc),
    prior_pay_amt = stg_claim_insurance.prior_pay_amt,
    est_due_amt = stg_claim_insurance.est_due_amt,
    other_provider_id = TRIM(stg_claim_insurance.other_provider_id),
    insured_name = TRIM(stg_claim_insurance.insured_name),
    pat_to_ins_rel_ind = TRIM(stg_claim_insurance.pat_to_ins_rel_ind),
    insured_id = TRIM(stg_claim_insurance.insured_id),
    insured_group_name = TRIM(stg_claim_insurance.insured_group_name),
    insured_group_num = TRIM(stg_claim_insurance.insured_group_num),
    treatment_auth_code = TRIM(stg_claim_insurance.treatment_auth_code),
    doc_cntrl_num = TRIM(stg_claim_insurance.doc_cntrl_num),
    employer_name = TRIM(stg_claim_insurance.employer_name)
    WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        payor_seq_ind,
        iplan_id,
        payer_id,
        payer_sub_id,
        payer_name,
        health_plan_id,
        release_info_cert_desc,
        assign_benefit_cert_desc,
        prior_pay_amt,
        est_due_amt,
        other_provider_id,
        insured_name,
        pat_to_ins_rel_ind,
        insured_id,
        insured_group_name,
        insured_group_num,
        treatment_auth_code,
        doc_cntrl_num,
        employer_name,
        dw_last_update_date_time,
        source_system_code)
VALUES (TRIM(stg_claim_insurance.claim_id), TRIM(stg_claim_insurance.payor_seq_ind), TRIM(stg_claim_insurance.iplan_id),
TRIM(stg_claim_insurance.payer_id), TRIM(stg_claim_insurance.payer_sub_id), TRIM(stg_claim_insurance.payer_name),
TRIM(stg_claim_insurance.health_plan_id), TRIM(stg_claim_insurance.release_info_cert_desc), TRIM(stg_claim_insurance.assign_benefit_cert_desc),
stg_claim_insurance.prior_pay_amt, stg_claim_insurance.est_due_amt, TRIM(stg_claim_insurance.other_provider_id), 
TRIM(stg_claim_insurance.insured_name), TRIM(stg_claim_insurance.pat_to_ins_rel_ind), TRIM(stg_claim_insurance.insured_id), 
TRIM(stg_claim_insurance.insured_group_name), TRIM(stg_claim_insurance.insured_group_num), TRIM(stg_claim_insurance.treatment_auth_code), 
TRIM(stg_claim_insurance.doc_cntrl_num), TRIM(stg_claim_insurance.employer_name), datetime_trunc(current_datetime('US/Central'), SECOND), 
TRIM(stg_claim_insurance.source_system_code));

SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id,
             payor_seq_ind
      FROM {{ params.param_clm_core_dataset_name }}.fact_claim_insurance
      GROUP BY claim_id,
               payor_seq_ind
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_claim_insurance');

ELSE
COMMIT TRANSACTION;

END IF;