-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/dim_diagnosis_related_group.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.dim_diagnosis_related_group AS SELECT
    dim_diagnosis_related_group.drg_code,
    dim_diagnosis_related_group.drg_desc,
    dim_diagnosis_related_group.drg_type,
    dim_diagnosis_related_group.drg_weight,
    dim_diagnosis_related_group.drg_geometric_mean_los,
    dim_diagnosis_related_group.drg_arithmetic_mean_los,
    dim_diagnosis_related_group.major_diagnosis_category_code,
    dim_diagnosis_related_group.valid_from_date_time,
    dim_diagnosis_related_group.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_diagnosis_related_group
;
