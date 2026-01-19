CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_lookup_groups_stg (
group_id INT64
, lookup_id INT64
, beginhisto INT64
, endhisto INT64
, beginprimarysite INT64
, endprimarysite INT64
, beginrxtype INT64
, endrxtype INT64
, dw_last_update_date_time DATETIME
)
  ;
