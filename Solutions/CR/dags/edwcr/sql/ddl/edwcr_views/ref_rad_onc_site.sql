-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_rad_onc_site.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_rad_onc_site AS SELECT
    a.site_sk,
    a.source_site_id,
    a.source_site_guid_text,
    a.site_code_text,
    a.site_name,
    a.server_name,
    a.site_desc,
    a.server_ip_address_text,
    a.aura_version_text,
    a.aura_last_installed_date_time,
    a.registration_date_time,
    a.history_user_name,
    a.history_date_time,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS a
;
