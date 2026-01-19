-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_registry_fact_patient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_registry_fact_patient AS SELECT
    junc_registry_fact_patient.patient_dw_id,
    junc_registry_fact_patient.registry_code,
    junc_registry_fact_patient.coid,
    junc_registry_fact_patient.company_code,
    junc_registry_fact_patient.source_system_code,
    junc_registry_fact_patient.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.junc_registry_fact_patient
;
