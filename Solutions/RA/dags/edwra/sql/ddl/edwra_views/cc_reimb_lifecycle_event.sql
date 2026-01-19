-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_reimb_lifecycle_event.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_reimb_lifecycle_event AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.log_sequence_num,
    a.eff_from_date,
    a.reimb_lifecycle_id,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_id,
    a.lifecycle_date,
    a.expected_payment_amt,
    a.actual_payment_amt,
    a.payor_due_amt,
    a.lifecycle_event_type_id,
    a.account_payer_status_id,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_reimb_lifecycle_event AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
