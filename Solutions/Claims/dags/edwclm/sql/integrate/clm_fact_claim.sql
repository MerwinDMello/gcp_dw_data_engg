DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_claim.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_claim AS core_claim
USING {{ params.param_clm_stage_dataset_name }}.fact_claim AS stg_claim
ON upper(trim(core_claim.claim_id, ' ')) = upper(trim(stg_claim.claim_id, ' '))
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    vendor_cid = TRIM(stg_claim.vendor_cid),
    unit_num = TRIM(stg_claim.unit_num),
    bill_type_code = TRIM(stg_claim.bill_type_code),
    bill_provider_sid = stg_claim.bill_provider_sid,
    pay_to_provider_sid = stg_claim.pay_to_provider_sid,
    total_charge_amt = stg_claim.total_charge_amt,
    payor_seq_ind = TRIM(stg_claim.payor_seq_ind),
    iplan_id = TRIM(stg_claim.iplan_id),
    bill_dt = stg_claim.bill_dt,
    patient_acct_num = TRIM(stg_claim.patient_acct_num),
    med_rec_num = TRIM(stg_claim.med_rec_num),
    fed_tax_num = TRIM(stg_claim.fed_tax_num),
    stmt_cover_from_dt = stg_claim.stmt_cover_from_dt,
    stmt_cover_to_dt = stg_claim.stmt_cover_to_dt,
    admission_dt = stg_claim.admission_dt,
    admission_hr = TRIM(stg_claim.admission_hr),
    admission_type_ind = TRIM(stg_claim.admission_type_ind),
    admission_source_cd = TRIM(stg_claim.admission_source_cd),
    discharge_hr = TRIM(stg_claim.discharge_hr),
    discharge_status_cd = TRIM(stg_claim.discharge_status_cd),
    accident_st = TRIM(stg_claim.accident_st),
    npi = stg_claim.npi,
    admit_diag_code = TRIM(stg_claim.admit_diag_code),
    drg = TRIM(stg_claim.drg),
    claim_desc = TRIM(stg_claim.claim_desc),
    file_link_path_txt = TRIM(stg_claim.file_link_path_txt),
    claim_file_name = TRIM(stg_claim.claim_file_name),
    source_system_code = TRIM(stg_claim.source_system_code),
    prefix_pat_acct_num = TRIM(stg_claim.prefix_pat_acct_num),
    pas_coid = TRIM(stg_claim.pas_coid),
    financial_class = stg_claim.financial_class,
    patient_type = TRIM(stg_claim.patient_type),
    edi_837_type = TRIM(stg_claim.edi_837_type),
    numeric_unit_num = stg_claim.numeric_unit_num,
    numeric_patient_acct_num = stg_claim.numeric_patient_acct_num,
    service_code = TRIM(stg_claim.service_code),
    taxonomy_code = TRIM(stg_claim.taxonomy_code),
    destination_method = TRIM(stg_claim.destination_method),
    operationalgroup = TRIM(stg_claim.operationalgroup)
WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        vendor_cid,
        unit_num,
        bill_type_code,
        bill_provider_sid,
        pay_to_provider_sid,
        total_charge_amt,
        payor_seq_ind,
        iplan_id,
        bill_dt,
        patient_acct_num,
        med_rec_num,
        fed_tax_num,
        stmt_cover_from_dt,
        stmt_cover_to_dt,
        admission_dt,
        admission_hr,
        admission_type_ind,
        admission_source_cd,
        discharge_hr,
        discharge_status_cd,
        accident_st,
        npi,
        admit_diag_code,
        drg,
        claim_desc,
        file_link_path_txt,
        claim_file_name,
        dw_last_update_date_time,
        source_system_code,
        prefix_pat_acct_num,
        pas_coid,
        financial_class,
        patient_type,
        edi_837_type,
        numeric_unit_num,
        numeric_patient_acct_num,
        service_code,
        taxonomy_code,
        destination_method,
        operationalgroup)
VALUES (TRIM(stg_claim.claim_id), TRIM(stg_claim.vendor_cid), TRIM(stg_claim.unit_num), TRIM(stg_claim.bill_type_code), stg_claim.bill_provider_sid, stg_claim.pay_to_provider_sid, stg_claim.total_charge_amt, TRIM(stg_claim.payor_seq_ind), TRIM(stg_claim.iplan_id), stg_claim.bill_dt, TRIM(stg_claim.patient_acct_num), TRIM(stg_claim.med_rec_num), TRIM(stg_claim.fed_tax_num), stg_claim.stmt_cover_from_dt, stg_claim.stmt_cover_to_dt, stg_claim.admission_dt, TRIM(stg_claim.admission_hr), TRIM(stg_claim.admission_type_ind), TRIM(stg_claim.admission_source_cd), TRIM(stg_claim.discharge_hr), TRIM(stg_claim.discharge_status_cd), TRIM(stg_claim.accident_st), stg_claim.npi, TRIM(stg_claim.admit_diag_code), TRIM(stg_claim.drg), TRIM(stg_claim.claim_desc), TRIM(stg_claim.file_link_path_txt), TRIM(stg_claim.claim_file_name), datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(stg_claim.source_system_code), TRIM(stg_claim.prefix_pat_acct_num), TRIM(stg_claim.pas_coid), stg_claim.financial_class, TRIM(stg_claim.patient_type), TRIM(stg_claim.edi_837_type), stg_claim.numeric_unit_num, stg_claim.numeric_patient_acct_num, TRIM(stg_claim.service_code), TRIM(stg_claim.taxonomy_code), TRIM(stg_claim.destination_method), TRIM(operationalgroup));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id
      FROM {{ params.param_clm_core_dataset_name }}.fact_claim
      GROUP BY claim_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_claim');

ELSE
COMMIT TRANSACTION;

END IF;