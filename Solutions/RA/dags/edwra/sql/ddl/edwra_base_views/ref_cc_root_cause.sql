-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_root_cause AS SELECT
    ref_cc_root_cause.company_code,
    ref_cc_root_cause.coid,
    ref_cc_root_cause.root_cause_id,
    ref_cc_root_cause.root_cause_code,
    ref_cc_root_cause.root_cause_desc,
    ref_cc_root_cause.create_login_userid,
    ref_cc_root_cause.create_date_time,
    ref_cc_root_cause.update_login_userid,
    ref_cc_root_cause.update_date_time,
    ref_cc_root_cause.inactive_date,
    ref_cc_root_cause.active_ind,
    ref_cc_root_cause.dw_last_update_date_time,
    ref_cc_root_cause.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_root_cause
;
