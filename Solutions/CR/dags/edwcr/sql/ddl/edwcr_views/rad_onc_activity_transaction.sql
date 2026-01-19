-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/rad_onc_activity_transaction.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.rad_onc_activity_transaction AS SELECT
    a.activity_transaction_sk,
    a.activity_priority_sk,
    a.hospital_sk,
    a.patient_sk,
    a.activity_sk,
    a.appointment_status_id,
    a.actual_resource_type_id,
    a.cancel_reason_type_id,
    a.appointment_resource_status_id,
    a.site_sk,
    a.source_activity_transaction_id,
    a.schedule_end_date_time,
    a.appointment_date_time,
    a.appointment_schedule_ind,
    a.activity_start_date_time,
    a.activity_end_date_time,
    a.activity_note_text,
    a.check_in_ind,
    a.patient_arrival_date_time,
    a.waitlist_ind,
    a.patient_location_text,
    a.appointment_instance_ind,
    a.appointment_task_date_time,
    a.activity_owner_ind,
    a.visit_type_open_chart_ind,
    a.resource_id,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.rad_onc_activity_transaction AS a
;
