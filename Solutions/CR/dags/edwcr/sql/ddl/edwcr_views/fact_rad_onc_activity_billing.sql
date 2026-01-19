-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_rad_onc_activity_billing.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_rad_onc_activity_billing AS SELECT
    a.fact_activity_billing_sk,
    a.physician_sk,
    a.attending_oncologist_id,
    a.patient_course_sk,
    a.hospital_sk,
    a.activity_sk,
    a.activity_transaction_sk,
    a.procedure_code_sk,
    a.patient_sk,
    a.activity_category_id,
    a.site_sk,
    a.source_fact_activity_billing_id,
    a.primary_global_charge_amt,
    a.primary_technical_charge_amt,
    a.primary_professional_charge_amt,
    a.other_professional_charge_amt,
    a.other_technical_charge_amt,
    a.other_global_charge_amt,
    a.forecast_charge_amt,
    a.actual_charge_amt,
    a.activity_cost_amt,
    a.activity_billing_code_text,
    a.service_start_date_time,
    a.service_end_date_time,
    a.complete_date_time,
    a.export_date_time,
    a.mark_complete_date_time,
    a.credit_export_date_time,
    a.credit_date_time,
    a.credit_note_text,
    a.modifier_code_text,
    a.credit_amt,
    a.scheduled_ind,
    a.object_status_ind,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_activity_billing AS a
;
