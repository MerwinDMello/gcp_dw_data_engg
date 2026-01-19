-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_rad_onc_activity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_rad_onc_activity AS SELECT
    a.activity_sk,
    a.activity_object_status_id,
    a.site_sk,
    a.source_activity_id,
    a.activity_category_code_text,
    a.activity_category_desc,
    a.activity_code_text,
    a.activity_desc,
    a.source_last_modified_date_time,
    a.log_id,
    a.run_id,
    a.effective_start_date_time,
    a.effective_end_date_time,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_activity AS a
;
