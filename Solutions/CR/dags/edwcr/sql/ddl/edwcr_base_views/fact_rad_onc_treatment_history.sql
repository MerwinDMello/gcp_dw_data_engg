CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_treatment_history
   OPTIONS(description='Contains fact information of Radiation Oncology for treatment history')
  AS SELECT
      fact_rad_onc_treatment_history.fact_treatment_history_sk,
      fact_rad_onc_treatment_history.patient_course_sk,
      fact_rad_onc_treatment_history.patient_plan_sk,
      fact_rad_onc_treatment_history.patient_sk,
      fact_rad_onc_treatment_history.treatment_intent_type_id,
      fact_rad_onc_treatment_history.clinical_status_id,
      fact_rad_onc_treatment_history.plan_status_id,
      fact_rad_onc_treatment_history.field_technique_id,
      fact_rad_onc_treatment_history.technique_id,
      fact_rad_onc_treatment_history.technique_label_id,
      fact_rad_onc_treatment_history.technique_delivery_type_id,
      fact_rad_onc_treatment_history.site_sk,
      fact_rad_onc_treatment_history.source_fact_treatment_history_id,
      fact_rad_onc_treatment_history.completion_date_time,
      fact_rad_onc_treatment_history.first_treatment_date_time,
      fact_rad_onc_treatment_history.last_treatment_date_time,
      fact_rad_onc_treatment_history.status_date_time,
      fact_rad_onc_treatment_history.active_ind,
      fact_rad_onc_treatment_history.planned_dose_rate_num,
      fact_rad_onc_treatment_history.course_dose_delivered_amt,
      fact_rad_onc_treatment_history.course_dose_planned_amt,
      fact_rad_onc_treatment_history.course_dose_remaining_amt,
      fact_rad_onc_treatment_history.other_course_dose_delivered_amt,
      fact_rad_onc_treatment_history.dose_correction_amt,
      fact_rad_onc_treatment_history.total_dose_limit_amt,
      fact_rad_onc_treatment_history.daily_dose_limit_amt,
      fact_rad_onc_treatment_history.session_dose_limit_amt,
      fact_rad_onc_treatment_history.primary_ind,
      fact_rad_onc_treatment_history.log_id,
      fact_rad_onc_treatment_history.run_id,
      fact_rad_onc_treatment_history.source_system_code,
      fact_rad_onc_treatment_history.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.fact_rad_onc_treatment_history
  ;
