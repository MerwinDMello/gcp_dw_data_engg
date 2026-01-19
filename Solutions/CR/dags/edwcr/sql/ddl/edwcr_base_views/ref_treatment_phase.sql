CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_treatment_phase
   OPTIONS(description='Contains the distinct list of treatment phase')
  AS SELECT
      ref_treatment_phase.treatment_phase_id,
      ref_treatment_phase.treatment_phase_desc,
      ref_treatment_phase.source_system_code,
      ref_treatment_phase.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_treatment_phase
  ;
