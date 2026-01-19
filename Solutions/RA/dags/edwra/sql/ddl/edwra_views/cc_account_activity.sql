-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_account_activity
   OPTIONS(description='Activities related to active and resolved discrepancies.  When the discrepancy is resolved, the resolved date will be a current or past actual date.  When the discrepancy is active or un-resolved, the resolved date will be a future date, \'9999-12-31\'.')
  AS SELECT
      a.patient_dw_id,
      a.activity_id,
      a.company_code,
      a.coid,
      a.unit_num,
      a.payor_dw_id,
      a.iplan_insurance_order_num,
      a.pat_acct_num,
      a.iplan_id,
      a.activity_create_date_time,
      a.activity_update_date_time,
      a.create_login_userid,
      a.activity_subject_text,
      a.activity_desc,
      a.expected_duration_num,
      a.activity_due_date,
      a.activity_owner_login_userid,
      a.create_appeal_or_coll_ind,
      a.activity_type_id,
      a.activity_status_id,
      a.activity_complete_date_time,
      a.complete_login_userid,
      a.activity_resolve_text,
      a.activity_source_code,
      a.active_ind,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
