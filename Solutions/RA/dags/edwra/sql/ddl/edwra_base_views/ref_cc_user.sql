-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_user
   OPTIONS(description='Concuity User Reference table')
  AS SELECT
      ref_cc_user.company_code,
      ref_cc_user.coid,
      ref_cc_user.user_id,
      ref_cc_user.customer_org_id,
      ref_cc_user.user_first_nm,
      ref_cc_user.user_last_nm,
      ref_cc_user.user_title_nm,
      ref_cc_user.user_email_addr,
      ref_cc_user.user_expire_password_ind,
      ref_cc_user.user_password_expire_dt,
      ref_cc_user.user_is_active_ind,
      ref_cc_user.user_login_id,
      ref_cc_user.user_role_nm,
      ref_cc_user.user_default_summary_id,
      ref_cc_user.user_creation_dt,
      ref_cc_user.user_created_by_id,
      ref_cc_user.user_action_emails_ind,
      ref_cc_user.user_mod_by_user_id,
      ref_cc_user.user_mod_date,
      ref_cc_user.user_dual_access_ind,
      ref_cc_user.source_system_code,
      ref_cc_user.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user
  ;
