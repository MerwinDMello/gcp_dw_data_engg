-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_practitioner_id.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_practitioner_id AS SELECT
    dim_practitioner_id.practitioner_sk,
    dim_practitioner_id.registration_code,
    dim_practitioner_id.id_txt,
    dim_practitioner_id.valid_from_date_time,
    dim_practitioner_id.role_plyr_dw_id,
    dim_practitioner_id.id_type_code,
    dim_practitioner_id.source_system_txt,
    dim_practitioner_id.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_practitioner_id
;
