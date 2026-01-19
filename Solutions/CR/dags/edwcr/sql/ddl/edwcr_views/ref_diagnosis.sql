-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_diagnosis AS SELECT
    ref_diagnosis.diag_code,
    ref_diagnosis.diag_type_code,
    ref_diagnosis.drg_major_diag_cat_code,
    ref_diagnosis.diagnosis_desc,
    ref_diagnosis.diagnosis_short_desc,
    ref_diagnosis.eff_from_date,
    ref_diagnosis.eff_to_date,
    ref_diagnosis.sex_edit_indicator,
    ref_diagnosis.external_cause_ind,
    ref_diagnosis.source_system_code
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_diagnosis
;
