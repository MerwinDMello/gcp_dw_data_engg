-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_eor_amount.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS SELECT
    cc_eor_amount.company_code,
    cc_eor_amount.coid,
    cc_eor_amount.patient_dw_id,
    cc_eor_amount.payor_dw_id,
    cc_eor_amount.iplan_insurance_order_num,
    cc_eor_amount.eor_log_date,
    cc_eor_amount.log_id,
    cc_eor_amount.log_sequence_num,
    cc_eor_amount.eff_from_date,
    cc_eor_amount.reimbursement_method_type_code,
    cc_eor_amount.amount_category_code,
    cc_eor_amount.unit_num,
    cc_eor_amount.pat_acct_num,
    cc_eor_amount.iplan_id,
    cc_eor_amount.eor_reimbursement_amt,
    cc_eor_amount.dw_last_update_date_time,
    cc_eor_amount.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_amount
;
