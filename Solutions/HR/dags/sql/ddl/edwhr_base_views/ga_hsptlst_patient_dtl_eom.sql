-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/ga_hsptlst_patient_dtl_eom.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ga_hsptlst_patient_dtl_eom AS SELECT
    ga_hsptlst_patient_dtl_eom.patient_dw_id,
    ga_hsptlst_patient_dtl_eom.company_code,
    ga_hsptlst_patient_dtl_eom.coid,
    ga_hsptlst_patient_dtl_eom.unit_num,
    ga_hsptlst_patient_dtl_eom.pat_acct_num,
    ga_hsptlst_patient_dtl_eom.medical_record_num,
    ga_hsptlst_patient_dtl_eom.source_system_code,
    ga_hsptlst_patient_dtl_eom.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.ga_hsptlst_patient_dtl_eom
;
