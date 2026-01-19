-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/patient_visit_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.patient_visit_reason AS SELECT
    patient_visit_reason.coid,
    patient_visit_reason.company_code,
    patient_visit_reason.patient_dw_id,
    patient_visit_reason.visit_reason_rank_num,
    patient_visit_reason.diag_type_code,
    patient_visit_reason.visit_reason_code,
    patient_visit_reason.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.patient_visit_reason
;
