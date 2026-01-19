-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_user
   OPTIONS(description='Concuity User Reference table')
  AS SELECT
      a.company_code,
      a.coid,
      a.user_id,
      a.customer_org_id,
      a.user_first_nm,
      a.user_last_nm,
      a.user_title_nm,
      a.user_email_addr,
      a.user_expire_password_ind,
      a.user_password_expire_dt,
      a.user_is_active_ind,
      a.user_login_id,
      a.user_role_nm,
      a.user_default_summary_id,
      a.user_creation_dt,
      a.user_created_by_id,
      a.user_action_emails_ind,
      a.user_mod_by_user_id,
      a.user_mod_date,
      a.user_dual_access_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_user AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
