CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_patient_payor
   OPTIONS(description='Payor information at the Patient level. Contains the primary insurance of the patient.')
  AS SELECT
      cdm_patient_payor.patient_dw_id,
      cdm_patient_payor.coid,
      cdm_patient_payor.company_code,
      cdm_patient_payor.pat_acct_num,
      cdm_patient_payor.payor_dw_id_ins1,
      cdm_patient_payor.payor_name,
      cdm_patient_payor.iplan_id_ins1,
      cdm_patient_payor.plan_desc,
      cdm_patient_payor.major_payor_id_ins1,
      cdm_patient_payor.major_payor_group_desc,
      cdm_patient_payor.financial_class_code,
      cdm_patient_payor.financial_class_desc,
      cdm_patient_payor.source_system_code,
      cdm_patient_payor.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_patient_payor
  ;
