CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.rad_onc_address_wrk (
address_sk INT64
, address_line_1_text STRING
, address_line_2_text STRING
, full_address_text STRING
, address_comment_text STRING
)
  ;
