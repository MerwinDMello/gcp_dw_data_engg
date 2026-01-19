-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_remittance_remark.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance_remark AS SELECT
    cc_remittance_remark.company_code,
    cc_remittance_remark.coid,
    cc_remittance_remark.patient_dw_id,
    cc_remittance_remark.payor_dw_id,
    cc_remittance_remark.remittance_advice_num,
    cc_remittance_remark.ra_log_date,
    cc_remittance_remark.log_id,
    cc_remittance_remark.log_sequence_num,
    cc_remittance_remark.remark_code_type,
    cc_remittance_remark.remark_code_seq,
    cc_remittance_remark.unit_num,
    cc_remittance_remark.pat_acct_num,
    cc_remittance_remark.iplan_insurance_order_num,
    cc_remittance_remark.iplan_id,
    cc_remittance_remark.remark_code,
    cc_remittance_remark.dw_last_update_date_time,
    cc_remittance_remark.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_remark
;
