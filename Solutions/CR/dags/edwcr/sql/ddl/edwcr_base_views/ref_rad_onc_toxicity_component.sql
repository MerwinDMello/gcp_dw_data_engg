CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_toxicity_component
   OPTIONS(description='Contains information for radiation oncology toxicity component')
  AS SELECT
      ref_rad_onc_toxicity_component.toxicity_component_sk,
      ref_rad_onc_toxicity_component.toxicity_component_name,
      ref_rad_onc_toxicity_component.source_system_code,
      ref_rad_onc_toxicity_component.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_component
  ;
