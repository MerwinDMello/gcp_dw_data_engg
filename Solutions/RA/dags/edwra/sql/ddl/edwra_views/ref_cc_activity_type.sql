-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_activity_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_activity_type AS SELECT
    a.company_code,
    a.coid,
    a.activity_type_id,
    a.activity_type_desc,
    a.activity_type_name,
    a.default_days_num,
    a.pay_days_num,
    a.auto_complete_ind,
    a.incl_rev_recov_rpt_ind,
    a.incl_notes_ind,
    a.create_appeal_collect_ind,
    a.follow_up_activity_type_id,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_activity_type AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
