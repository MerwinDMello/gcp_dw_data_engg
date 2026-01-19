-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_appeal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_appeal
   OPTIONS(description='Concuity Appeals, contains account appeal details.')
  AS SELECT
      cc_appeal.company_code,
      cc_appeal.coid,
      cc_appeal.patient_dw_id,
      cc_appeal.payor_dw_id,
      cc_appeal.iplan_insurance_order_num,
      cc_appeal.appeal_num,
      cc_appeal.unit_num,
      cc_appeal.pat_acct_num,
      cc_appeal.iplan_id,
      cc_appeal.appeal_amt,
      cc_appeal.appeal_origination_date,
      cc_appeal.appeal_close_date,
      cc_appeal.denied_days_num,
      cc_appeal.appeal_id,
      cc_appeal.appeal_create_user_id,
      cc_appeal.appeal_create_date_time,
      cc_appeal.appeal_update_user_id,
      cc_appeal.appeal_update_date_time,
      cc_appeal.appeal_reopen_user_id,
      cc_appeal.appeal_reopen_date_time,
      cc_appeal.dw_last_update_date_time,
      cc_appeal.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
  ;
