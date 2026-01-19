-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_group.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_group AS SELECT
    dim_group.group_sk,
    dim_group.group_name,
    dim_group.valid_from_date_time,
    dim_group.external_ind,
    dim_group.active_ind,
    dim_group.source_system_code,
    dim_group.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_group
;
