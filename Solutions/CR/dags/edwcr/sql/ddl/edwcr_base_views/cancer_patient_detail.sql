CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_detail
   OPTIONS(description='Contains details of consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID')
  AS SELECT
      cancer_patient_detail.cancer_patient_detail_sk,
      cancer_patient_detail.cancer_patient_driver_sk,
      cancer_patient_detail.cr_patient_id,
      cancer_patient_detail.cn_patient_id,
      cancer_patient_detail.cp_patient_id,
      cancer_patient_detail.coid,
      cancer_patient_detail.company_code,
      cancer_patient_detail.network_mnemonic_cs,
      cancer_patient_detail.patient_market_urn_text,
      cancer_patient_detail.medical_record_num,
      cancer_patient_detail.relationship_name,
      cancer_patient_detail.address_line_1_text,
      cancer_patient_detail.address_line_2_text,
      cancer_patient_detail.city_name,
      cancer_patient_detail.death_date,
      cancer_patient_detail.patient_birth_date,
      cancer_patient_detail.patient_email_address_text,
      cancer_patient_detail.patient_gender_code,
      cancer_patient_detail.patient_state_code,
      cancer_patient_detail.zip_code,
      cancer_patient_detail.phone_num_type_code,
      cancer_patient_detail.phone_num,
      cancer_patient_detail.preferred_language_text,
      cancer_patient_detail.insurance_type_name,
      cancer_patient_detail.preferred_contact_method_text,
      cancer_patient_detail.insurance_company_name,
      cancer_patient_detail.race_name,
      cancer_patient_detail.patient_system_id,
      cancer_patient_detail.vital_status_name,
      cancer_patient_detail.source_system_code,
      cancer_patient_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_detail
  ;
