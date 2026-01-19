-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_tumor_site_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
/***************************************************************************************
   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_tumor_site_diagnosis AS SELECT
    a.diagnosis_code,
    a.diagnosis_type_code,
    a.eff_from_date,
    a.eff_to_date,
    a.tumor_site_id,
    a.detail_tumor_site_id,
    a.tumor_type_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_tumor_site_diagnosis AS a
;
