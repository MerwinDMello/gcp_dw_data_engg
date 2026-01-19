-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_eor_op_pps.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_op_pps AS SELECT
    cc_eor_op_pps.company_code,
    cc_eor_op_pps.coid,
    cc_eor_op_pps.patient_dw_id,
    cc_eor_op_pps.payor_dw_id,
    cc_eor_op_pps.iplan_insurance_order_num,
    cc_eor_op_pps.eor_log_date,
    cc_eor_op_pps.log_id,
    cc_eor_op_pps.log_sequence_num,
    cc_eor_op_pps.eff_from_date,
    cc_eor_op_pps.op_pps_svc_cat_type_code,
    cc_eor_op_pps.unit_num,
    cc_eor_op_pps.pat_acct_num,
    cc_eor_op_pps.iplan_id,
    cc_eor_op_pps.op_pps_charge_amt,
    cc_eor_op_pps.op_pps_payment_amt,
    cc_eor_op_pps.op_pps_coinsurance_amt,
    cc_eor_op_pps.dw_last_update_date_time,
    cc_eor_op_pps.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_op_pps
;
