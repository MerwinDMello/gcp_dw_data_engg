CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimdrug (
dimsiteid INT64
, dimdrugid INT64
, unitofmeasure STRING
, agentname STRING
, medispandrugname STRING
, preferredagentname STRING
, agentclassdescription STRING
, route STRING
, form STRING
, strength STRING
, ctrdrug_desc_id STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
