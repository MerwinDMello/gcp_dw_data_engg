-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_appeal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_appeal
   OPTIONS(description='Concuity Appeals, contains account appeal details.')
  AS SELECT
      a.company_code,
      a.coid,
      a.patient_dw_id,
      a.payor_dw_id,
      a.iplan_insurance_order_num,
      a.appeal_num,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.appeal_amt,
      a.appeal_origination_date,
      a.appeal_close_date,
      a.denied_days_num,
      a.appeal_id,
      a.appeal_create_user_id,
      a.appeal_create_date_time,
      a.appeal_update_user_id,
      a.appeal_update_date_time,
      a.appeal_reopen_user_id,
      a.appeal_reopen_date_time,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_appeal AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
