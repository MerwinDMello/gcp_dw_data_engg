/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_encounter AS 
SELECT  
    encounter_sk,
    patient_dw_id,
    pat_acct_num,
    coid,
    en.company_code,
    patient_sk,
    facility_sk,
    attending_practitioner_sk,
    consulting_practitioner_sk,
    admitting_practitioner_sk,
    referring_practitioner_sk,
    discharge_status_code,
    discharge_status_desc,
    arrival_mode_code,
    arrival_mode_desc,
    admit_source_code,
    admit_source_desc,
    medical_record_num,
    patient_market_urn,
    visit_type_code,
    visit_type_desc,
    admit_type_code,
    patient_class_code,
    patient_status_code,
    priority_code,
    reason_for_visit_txt,
    emr_patient_id,
    encounter_id_txt,
    special_program_code,
    special_program_desc,
    vip_ind,
    encounter_date_time,
    admission_date_time,
    discharge_date_time,
    actl_los_cnt,
    accident_date_time,
    dbmotion_effective_date,
    expected_num_of_ins_plns_cnt,
    signature_date,
    readmission_ind,
    valid_from_date_time,
    eff_from_date,
    source_system_txt,
    source_system_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_encounter AS en
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON b.co_id = en.coid
     AND b.company_code = en.company_code