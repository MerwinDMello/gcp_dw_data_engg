-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
