CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure
   OPTIONS(description='Contains all the procedures a patient underwent.')
  AS SELECT
      cn_patient_procedure.cn_patient_procedure_sid,
      cn_patient_procedure.core_record_type_id,
      cn_patient_procedure.procedure_type_id,
      cn_patient_procedure.med_spcl_physician_id,
      cn_patient_procedure.nav_patient_id,
      cn_patient_procedure.tumor_type_id,
      cn_patient_procedure.diagnosis_result_id,
      cn_patient_procedure.nav_diagnosis_id,
      cn_patient_procedure.navigator_id,
      cn_patient_procedure.coid,
      cn_patient_procedure.company_code,
      cn_patient_procedure.procedure_date,
      cn_patient_procedure.palliative_ind,
      cn_patient_procedure.hashbite_ssk,
      cn_patient_procedure.source_system_code,
      cn_patient_procedure.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_procedure
  ;
