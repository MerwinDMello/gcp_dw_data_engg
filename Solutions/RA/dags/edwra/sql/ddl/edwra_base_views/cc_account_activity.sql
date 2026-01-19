-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity
   OPTIONS(description='Activities related to active and resolved discrepancies.  When the discrepancy is resolved, the resolved date will be a current or past actual date.  When the discrepancy is active or un-resolved, the resolved date will be a future date, \'9999-12-31\'.')
  AS SELECT
      cc_account_activity.patient_dw_id,
      cc_account_activity.activity_id,
      cc_account_activity.company_code,
      cc_account_activity.coid,
      cc_account_activity.unit_num,
      cc_account_activity.payor_dw_id,
      cc_account_activity.iplan_insurance_order_num,
      cc_account_activity.pat_acct_num,
      cc_account_activity.iplan_id,
      cc_account_activity.activity_create_date_time,
      cc_account_activity.activity_update_date_time,
      cc_account_activity.create_login_userid,
      cc_account_activity.activity_subject_text,
      cc_account_activity.activity_desc,
      cc_account_activity.expected_duration_num,
      cc_account_activity.activity_due_date,
      cc_account_activity.activity_owner_login_userid,
      cc_account_activity.create_appeal_or_coll_ind,
      cc_account_activity.activity_type_id,
      cc_account_activity.activity_status_id,
      cc_account_activity.activity_complete_date_time,
      cc_account_activity.complete_login_userid,
      cc_account_activity.activity_resolve_text,
      cc_account_activity.activity_source_code,
      cc_account_activity.active_ind,
      cc_account_activity.dw_last_update_date_time,
      cc_account_activity.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity
  ;
