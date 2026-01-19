-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_primary_language.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_primary_language AS SELECT
    dim_primary_language.primary_language_sk,
    dim_primary_language.primary_language_code,
    dim_primary_language.primary_language_desc,
    dim_primary_language.source_system_code,
    dim_primary_language.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_primary_language
;
