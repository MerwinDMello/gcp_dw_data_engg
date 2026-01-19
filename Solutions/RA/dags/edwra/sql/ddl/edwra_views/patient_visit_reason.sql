-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/patient_visit_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.patient_visit_reason AS SELECT
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.visit_reason_rank_num,
    a.diag_type_code,
    a.visit_reason_code,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.patient_visit_reason AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
