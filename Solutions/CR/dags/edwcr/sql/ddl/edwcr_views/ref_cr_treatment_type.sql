-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_cr_treatment_type.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_cr_treatment_type AS SELECT
    a.treatment_type_id,
    a.treatment_type_code,
    a.treatment_type_desc,
    a.treatment_group_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_cr_treatment_type AS a
;
