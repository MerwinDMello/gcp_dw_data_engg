-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_account_payor_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor_issue
   OPTIONS(description='The table Stores Issue reason details and the interface between Concuity and the PATH application keeps the two systems in sync.')
  AS SELECT
      cc_account_payor_issue.patient_dw_id,
      cc_account_payor_issue.payor_dw_id,
      cc_account_payor_issue.insurance_order_num,
      cc_account_payor_issue.coid,
      cc_account_payor_issue.acct_payer_issue_id,
      cc_account_payor_issue.company_code,
      cc_account_payor_issue.unit_num,
      cc_account_payor_issue.pat_acct_num,
      cc_account_payor_issue.iplan_id,
      cc_account_payor_issue.issue_rank_num,
      cc_account_payor_issue.acct_payer_id,
      cc_account_payor_issue.issue_id,
      cc_account_payor_issue.create_date,
      cc_account_payor_issue.create_user_name,
      cc_account_payor_issue.update_date,
      cc_account_payor_issue.update_user_name,
      cc_account_payor_issue.source_system_code,
      cc_account_payor_issue.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue
  ;
