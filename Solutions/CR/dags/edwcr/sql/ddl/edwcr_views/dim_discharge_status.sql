-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_discharge_status.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_discharge_status AS SELECT
    dim_discharge_status.discharge_status_sk,
    dim_discharge_status.discharge_status_code,
    dim_discharge_status.discharge_status_desc,
    dim_discharge_status.source_system_code,
    dim_discharge_status.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_discharge_status
;
