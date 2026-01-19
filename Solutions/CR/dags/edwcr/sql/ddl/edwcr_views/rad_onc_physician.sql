-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/rad_onc_physician.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.rad_onc_physician AS SELECT
    a.physician_sk,
    a.site_sk,
    a.source_physician_id,
    a.location_sk,
    a.resource_type_id,
    a.physician_first_name,
    a.physician_last_name,
    a.physician_suffix_name,
    a.physician_alias_name,
    a.physician_title_name,
    a.physician_email_address_text,
    a.physician_specialty_text,
    a.physician_id_text,
    a.resource_active_ind,
    a.appointment_schedule_ind,
    a.physician_start_date_time,
    a.physician_termination_date_time,
    a.physician_institution_text,
    a.physician_comment_text,
    a.resource_id,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.rad_onc_physician AS a
;
