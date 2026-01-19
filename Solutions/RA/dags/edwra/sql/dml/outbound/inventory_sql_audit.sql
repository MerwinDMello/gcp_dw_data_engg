EXPORT DATA OPTIONS(
  uri='gs://{{ params.param_parallon_ra_data_bucket_name }}/{{ params.param_parallon_ra_outbound_file_path }}{{ params.file_prefix }}{{ params.ssc_name_orig }}_*.txt',
  format='CSV',
  overwrite=true,
  header=false,
  field_delimiter='|') AS
SELECT
    ANY_VALUE(ssc_name),
    COUNT(*),
    SUM(payor_due_amount)
FROM {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_discrepancy_inventory
WHERE v_edw_daily_discrepancy_inventory.ssc_name = '{{ params.ssc_name }}'
;