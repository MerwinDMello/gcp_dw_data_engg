-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_anesthesia.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_anesthesia AS SELECT
    dim_anesthesia.anesthesia_sk,
    dim_anesthesia.anesthesia_code,
    dim_anesthesia.anesthesia_desc,
    dim_anesthesia.source_system_code,
    dim_anesthesia.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_anesthesia
;
