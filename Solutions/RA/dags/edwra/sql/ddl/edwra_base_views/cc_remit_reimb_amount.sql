-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_remit_reimb_amount.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remit_reimb_amount AS SELECT
    cc_remit_reimb_amount.company_code,
    cc_remit_reimb_amount.coid,
    cc_remit_reimb_amount.patient_dw_id,
    cc_remit_reimb_amount.payor_dw_id,
    cc_remit_reimb_amount.remittance_advice_num,
    cc_remit_reimb_amount.ra_log_date,
    cc_remit_reimb_amount.log_id,
    cc_remit_reimb_amount.log_sequence_num,
    cc_remit_reimb_amount.amount_category_code,
    cc_remit_reimb_amount.unit_num,
    cc_remit_reimb_amount.pat_acct_num,
    cc_remit_reimb_amount.iplan_insurance_order_num,
    cc_remit_reimb_amount.iplan_id,
    cc_remit_reimb_amount.reimbursement_amt,
    cc_remit_reimb_amount.dw_last_update_date_time,
    cc_remit_reimb_amount.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount
;
