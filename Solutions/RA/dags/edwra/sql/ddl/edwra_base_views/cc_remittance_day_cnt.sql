-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_remittance_day_cnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance_day_cnt AS SELECT
    cc_remittance_day_cnt.company_code,
    cc_remittance_day_cnt.coid,
    cc_remittance_day_cnt.patient_dw_id,
    cc_remittance_day_cnt.payor_dw_id,
    cc_remittance_day_cnt.remittance_advice_num,
    cc_remittance_day_cnt.ra_log_date,
    cc_remittance_day_cnt.log_id,
    cc_remittance_day_cnt.log_sequence_num,
    cc_remittance_day_cnt.remittance_day_code,
    cc_remittance_day_cnt.unit_num,
    cc_remittance_day_cnt.pat_acct_num,
    cc_remittance_day_cnt.iplan_insurance_order_num,
    cc_remittance_day_cnt.iplan_id,
    cc_remittance_day_cnt.remittance_day_cnt,
    cc_remittance_day_cnt.dw_last_update_date_time,
    cc_remittance_day_cnt.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt
;
