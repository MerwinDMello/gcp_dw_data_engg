-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_appeal_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_appeal_code AS SELECT
    a.company_code,
    a.coid,
    a.appeal_code_id,
    a.appeal_code,
    a.appeal_desc,
    a.appeal_category_id,
    a.pa_denial_code,
    a.user_assignable_ind,
    a.create_login_userid,
    a.create_date_time,
    a.update_login_userid,
    a.update_date_time,
    a.inactive_date,
    a.active_ind,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_appeal_code AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
