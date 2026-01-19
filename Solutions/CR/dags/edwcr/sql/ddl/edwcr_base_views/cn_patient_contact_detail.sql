CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_contact_detail
   OPTIONS(description='Contains detail communication data across different measures for a patient.')
  AS SELECT
      cn_patient_contact_detail.cn_patient_contact_sid,
      cn_patient_contact_detail.contact_detail_measure_type_id,
      cn_patient_contact_detail.contact_detail_measure_value_text,
      cn_patient_contact_detail.hashbite_ssk,
      cn_patient_contact_detail.source_system_code,
      cn_patient_contact_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_contact_detail
  ;
