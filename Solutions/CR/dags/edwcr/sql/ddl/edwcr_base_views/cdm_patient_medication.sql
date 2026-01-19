CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_patient_medication
   OPTIONS(description='Contains the information of the medication administered to the patient')
  AS SELECT
      cdm_patient_medication.medication_admn_sk,
      cdm_patient_medication.patient_dw_id,
      cdm_patient_medication.coid,
      cdm_patient_medication.company_code,
      cdm_patient_medication.medication_desc,
      cdm_patient_medication.occurence_ts,
      cdm_patient_medication.drug_dose_amt_text,
      cdm_patient_medication.drug_dose_measurement_text,
      cdm_patient_medication.administrative_frequency_text,
      cdm_patient_medication.administered_unit_cnt,
      cdm_patient_medication.route_code_sk,
      cdm_patient_medication.route_code_desc,
      cdm_patient_medication.ordering_physician_name,
      cdm_patient_medication.physician_npi,
      cdm_patient_medication.medication_num_text,
      cdm_patient_medication.source_system_original_code,
      cdm_patient_medication.clinical_pharmacy_trade_name,
      cdm_patient_medication.source_system_code,
      cdm_patient_medication.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_patient_medication
  ;
