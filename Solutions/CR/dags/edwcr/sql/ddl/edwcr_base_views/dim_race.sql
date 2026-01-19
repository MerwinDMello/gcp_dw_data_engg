CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_race AS SELECT
    dim_race.race_sk,
    dim_race.race_code,
    dim_race.race_desc,
    dim_race.source_system_code,
    dim_race.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_race
;
