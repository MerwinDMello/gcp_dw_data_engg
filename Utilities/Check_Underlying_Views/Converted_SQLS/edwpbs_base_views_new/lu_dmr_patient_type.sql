-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_dmr_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_dmr_patient_type AS SELECT
    lu_dmr_patient_type.dmr_patient_type_code,
    lu_dmr_patient_type.dmr_patient_type_desc,
    lu_dmr_patient_type.dw_last_update_date_time,
    lu_dmr_patient_type.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_dmr_patient_type
;
