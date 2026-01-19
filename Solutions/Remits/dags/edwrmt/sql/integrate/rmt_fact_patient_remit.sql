DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_patient_remit.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_patient_remit AS pfpr
USING {{ params.param_rmt_stage_dataset_name }}.fact_patient_remit AS sfpr
ON UPPER(TRIM(pfpr.patient_remit_sid, ' ')) = UPPER(TRIM(sfpr.patient_remit_sid, ' '))
WHEN MATCHED THEN
UPDATE
SET remit_id = TRIM(sfpr.remit_id),
    remit_provider_id = TRIM(sfpr.remit_provider_id),
    remit_payer_id = TRIM(sfpr.remit_payer_id),
    patient_acct_num = TRIM(sfpr.patient_acct_num),
    unit_num = TRIM(sfpr.unit_num),
    iplan = TRIM(sfpr.iplan),
    pas_coid = TRIM(sfpr.pas_coid),
    remit_file_path = TRIM(sfpr.remit_file_path),
    remit_file_name = TRIM(sfpr.remit_file_name),
    claim_file_name = TRIM(sfpr.claim_file_name),
    remit_bill_dt = sfpr.remit_bill_dt,
    remit_claim_status_cd = TRIM(sfpr.remit_claim_status_cd),
    remit_payment_amt = sfpr.remit_payment_amt,
    remit_claim_id = TRIM(sfpr.remit_claim_id),
    tot_claim_charge_amt = sfpr.tot_claim_charge_amt,
    patient_resp_amt = sfpr.patient_resp_amt,
    claim_file_inc_cd = TRIM(sfpr.claim_file_inc_cd),
    payer_claim_ctrll_num = TRIM(sfpr.payer_claim_ctrll_num),
    facility_cd = TRIM(sfpr.facility_cd),
    claim_freq_type_cd = TRIM(sfpr.claim_freq_type_cd),
    drg = TRIM(sfpr.drg),
    drg_weight = TRIM(sfpr.drg_weight),
    discharge_fraction = sfpr.discharge_fraction,
    dw_last_update_date_time = sfpr.dw_last_update_date_time,
    source_system_code = TRIM(sfpr.source_system_code),
    customer_cd = TRIM(sfpr.customer_cd) 
WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_remit_sid,
        remit_id,
        remit_provider_id,
        remit_payer_id,
        patient_acct_num,
        unit_num,
        iplan,
        pas_coid,
        remit_file_path,
        remit_file_name,
        claim_file_name,
        remit_bill_dt,
        remit_claim_status_cd,
        remit_payment_amt,
        remit_claim_id,
        tot_claim_charge_amt,
        patient_resp_amt,
        claim_file_inc_cd,
        payer_claim_ctrll_num,
        facility_cd,
        claim_freq_type_cd,
        drg,
        drg_weight,
        discharge_fraction,
        dw_last_update_date_time,
        source_system_code,
        customer_cd)
VALUES (TRIM(sfpr.patient_remit_sid), TRIM(sfpr.remit_id), TRIM(sfpr.remit_provider_id),
TRIM(sfpr.remit_payer_id), TRIM(sfpr.patient_acct_num), TRIM(sfpr.unit_num), TRIM(sfpr.iplan),
TRIM(sfpr.pas_coid), TRIM(sfpr.remit_file_path), TRIM(sfpr.remit_file_name), TRIM(sfpr.claim_file_name),
sfpr.remit_bill_dt, TRIM(sfpr.remit_claim_status_cd), sfpr.remit_payment_amt, TRIM(sfpr.remit_claim_id),
sfpr.tot_claim_charge_amt, sfpr.patient_resp_amt, TRIM(sfpr.claim_file_inc_cd),
TRIM(sfpr.payer_claim_ctrll_num), TRIM(sfpr.facility_cd), TRIM(sfpr.claim_freq_type_cd), TRIM(sfpr.drg),
TRIM(sfpr.drg_weight), sfpr.discharge_fraction, sfpr.dw_last_update_date_time,
TRIM(sfpr.source_system_code), TRIM(sfpr.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_remit_sid
      FROM {{ params.param_rmt_core_dataset_name }}.fact_patient_remit
      GROUP BY patient_remit_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_patient_remit');

ELSE
COMMIT TRANSACTION;

END IF;