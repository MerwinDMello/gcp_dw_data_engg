-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_patient_type_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_type_dim AS SELECT
    eis_patient_type_dim.patient_type_alias,
    eis_patient_type_dim.patient_type_gen02,
    eis_patient_type_dim.patient_type_gen02_alias,
    eis_patient_type_dim.patient_type_gen03,
    eis_patient_type_dim.patient_type_member,
    eis_patient_type_dim.patient_type_sid,
    eis_patient_type_dim.patient_type_gen03_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_type_dim
;
