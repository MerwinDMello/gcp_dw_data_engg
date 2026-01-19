-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_patient_fin_class_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_fin_class_dim AS SELECT
    eis_patient_fin_class_dim.patient_financial_class_sid,
    eis_patient_fin_class_dim.patient_financial_class_member,
    eis_patient_fin_class_dim.patient_financial_class_alias,
    eis_patient_fin_class_dim.patient_financial_class_gen02,
    eis_patient_fin_class_dim.patient_fin_class_gen02_alias,
    eis_patient_fin_class_dim.patient_financial_class_gen03,
    eis_patient_fin_class_dim.patient_fin_class_gen03_alias,
    eis_patient_fin_class_dim.patient_financial_class_gen04,
    eis_patient_fin_class_dim.patient_fin_class_gen04_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_fin_class_dim
;
