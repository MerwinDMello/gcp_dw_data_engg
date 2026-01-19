-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_account_payor_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_account_payor_issue
   OPTIONS(description='The table Stores Issue reason details and the interface between Concuity and the PATH application keeps the two systems in sync.')
  AS SELECT
      a.patient_dw_id,
      a.payor_dw_id,
      a.insurance_order_num,
      a.coid,
      a.acct_payer_issue_id,
      a.company_code,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.issue_rank_num,
      a.acct_payer_id,
      a.issue_id,
      a.create_date,
      a.create_user_name,
      a.update_date,
      a.update_user_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor_issue AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
