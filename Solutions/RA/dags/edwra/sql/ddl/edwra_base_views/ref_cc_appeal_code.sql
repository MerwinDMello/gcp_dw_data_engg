-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_appeal_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_appeal_code AS SELECT
    ref_cc_appeal_code.company_code,
    ref_cc_appeal_code.coid,
    ref_cc_appeal_code.appeal_code_id,
    ref_cc_appeal_code.appeal_code,
    ref_cc_appeal_code.appeal_desc,
    ref_cc_appeal_code.appeal_category_id,
    ref_cc_appeal_code.pa_denial_code,
    ref_cc_appeal_code.user_assignable_ind,
    ref_cc_appeal_code.create_login_userid,
    ref_cc_appeal_code.create_date_time,
    ref_cc_appeal_code.update_login_userid,
    ref_cc_appeal_code.update_date_time,
    ref_cc_appeal_code.inactive_date,
    ref_cc_appeal_code.active_ind,
    ref_cc_appeal_code.dw_last_update_date_time,
    ref_cc_appeal_code.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_code
;
