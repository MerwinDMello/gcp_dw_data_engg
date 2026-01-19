-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_reimb_lifecycle_event.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_reimb_lifecycle_event AS SELECT
    cc_reimb_lifecycle_event.company_code,
    cc_reimb_lifecycle_event.coid,
    cc_reimb_lifecycle_event.patient_dw_id,
    cc_reimb_lifecycle_event.payor_dw_id,
    cc_reimb_lifecycle_event.iplan_insurance_order_num,
    cc_reimb_lifecycle_event.eor_log_date,
    cc_reimb_lifecycle_event.log_id,
    cc_reimb_lifecycle_event.log_sequence_num,
    cc_reimb_lifecycle_event.eff_from_date,
    cc_reimb_lifecycle_event.reimb_lifecycle_id,
    cc_reimb_lifecycle_event.unit_num,
    cc_reimb_lifecycle_event.pat_acct_num,
    cc_reimb_lifecycle_event.iplan_id,
    cc_reimb_lifecycle_event.lifecycle_date,
    cc_reimb_lifecycle_event.expected_payment_amt,
    cc_reimb_lifecycle_event.actual_payment_amt,
    cc_reimb_lifecycle_event.payor_due_amt,
    cc_reimb_lifecycle_event.lifecycle_event_type_id,
    cc_reimb_lifecycle_event.account_payer_status_id,
    cc_reimb_lifecycle_event.dw_last_update_date_time,
    cc_reimb_lifecycle_event.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event
;
