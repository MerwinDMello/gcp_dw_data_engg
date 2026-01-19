CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient
   OPTIONS(description='Contains all the patients navigated by Sarah Cannon')
  AS SELECT
      cn_patient.nav_patient_id,
      cn_patient.navigator_id,
      cn_patient.gynecologist_physician_id,
      cn_patient.primary_care_physician_id,
      cn_patient.coid,
      cn_patient.company_code,
      cn_patient.patient_market_urn,
      cn_patient.medical_record_num,
      cn_patient.empi_text,
      cn_patient.facility_mnemonic_cs,
      cn_patient.network_mnemonic_cs,
      cn_patient.nav_create_date,
      cn_patient.source_system_code,
      cn_patient.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient
  ;
