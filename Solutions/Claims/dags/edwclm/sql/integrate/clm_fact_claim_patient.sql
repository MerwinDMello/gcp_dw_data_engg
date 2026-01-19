DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_claim_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_claim_patient AS core_claim_patient
USING {{ params.param_clm_stage_dataset_name }}.fact_claim_patient AS stg_claim_patient
ON upper(trim(core_claim_patient.claim_id, ' ')) = upper(trim(stg_claim_patient.claim_id, ' '))
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    patient_last_name = TRIM(stg_claim_patient.patient_last_name),
    patient_first_name = TRIM(stg_claim_patient.patient_first_name),
    patient_addr1 = TRIM(stg_claim_patient.patient_addr1),
    patient_addr2 = TRIM(stg_claim_patient.patient_addr2),
    patient_city = TRIM(stg_claim_patient.patient_city),
    patient_st = TRIM(stg_claim_patient.patient_st),
    patient_zip_cd = TRIM(stg_claim_patient.patient_zip_cd),
    patient_sex_cd = TRIM(stg_claim_patient.patient_sex_cd),
    patient_dob = stg_claim_patient.patient_dob,
    resp_party_name = TRIM(stg_claim_patient.resp_party_name),
    resp_party_addr1 = TRIM(stg_claim_patient.resp_party_addr1),
    resp_party_addr2 = TRIM(stg_claim_patient.resp_party_addr2),
    resp_party_city = TRIM(stg_claim_patient.resp_party_city),
    resp_party_st = TRIM(stg_claim_patient.resp_party_st),
    resp_party_zip_cd = TRIM(stg_claim_patient.resp_party_zip_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        patient_last_name,
        patient_first_name,
        patient_addr1,
        patient_addr2,
        patient_city,
        patient_st,
        patient_zip_cd,
        patient_sex_cd,
        patient_dob,
        resp_party_name,
        resp_party_addr1,
        resp_party_addr2,
        resp_party_city,
        resp_party_st,
        resp_party_zip_cd,
        dw_last_update_date_time,
        source_system_code)
VALUES (TRIM(stg_claim_patient.claim_id), TRIM(stg_claim_patient.patient_last_name), 
TRIM(stg_claim_patient.patient_first_name), TRIM(stg_claim_patient.patient_addr1), 
TRIM(stg_claim_patient.patient_addr2), TRIM(stg_claim_patient.patient_city), 
TRIM(stg_claim_patient.patient_st), TRIM(stg_claim_patient.patient_zip_cd), 
TRIM(stg_claim_patient.patient_sex_cd), stg_claim_patient.patient_dob, 
TRIM(stg_claim_patient.resp_party_name), TRIM(stg_claim_patient.resp_party_addr1), 
TRIM(stg_claim_patient.resp_party_addr2), TRIM(stg_claim_patient.resp_party_city), 
TRIM(stg_claim_patient.resp_party_st), TRIM(stg_claim_patient.resp_party_zip_cd), 
datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(stg_claim_patient.source_system_code));

SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id
      FROM {{ params.param_clm_core_dataset_name }}.fact_claim_patient
      GROUP BY claim_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_claim_patient');

ELSE
COMMIT TRANSACTION;

END IF;