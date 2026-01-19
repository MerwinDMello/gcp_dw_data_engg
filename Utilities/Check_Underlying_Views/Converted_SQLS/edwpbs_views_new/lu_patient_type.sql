-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_patient_type AS SELECT
    eis_patient_type_dim.patient_type_gen02 AS patient_type
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_type_dim
  WHERE eis_patient_type_dim.patient_type_gen02 IN(
    'Outpatient', 'Inpatient'
  )
  GROUP BY 1
;
