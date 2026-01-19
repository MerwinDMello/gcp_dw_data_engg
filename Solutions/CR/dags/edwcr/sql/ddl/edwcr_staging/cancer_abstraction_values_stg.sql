CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_abstraction_values_stg (
unique_message_id STRING
, abstraction_field STRING
, value_predicted STRING
, value_submitted STRING
, value_suggested STRING
, dw_last_update_date_time DATETIME
, site_and_associated_model_output_score STRING
, second_primary_site STRING
)
  ;
