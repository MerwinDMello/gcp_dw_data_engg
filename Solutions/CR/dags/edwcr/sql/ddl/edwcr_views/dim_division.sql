-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_division.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_division AS SELECT
    dim_division.division_sk,
    dim_division.division_name,
    dim_division.group_sk,
    dim_division.valid_from_date_time,
    dim_division.external_ind,
    dim_division.active_ind,
    dim_division.source_system_code,
    dim_division.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_division
;
