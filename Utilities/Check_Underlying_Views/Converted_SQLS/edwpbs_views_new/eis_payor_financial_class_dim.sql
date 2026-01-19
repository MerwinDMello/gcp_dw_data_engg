-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_payor_financial_class_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_payor_financial_class_dim AS SELECT
    a.payor_financial_class_alias,
    a.payor_financial_class_gen02,
    a.payor_financial_class_gen03,
    a.payor_financial_class_gen04,
    a.payor_financial_class_member,
    a.payor_financial_class_sid,
    a.payor_fin_class_gen02_alias,
    a.payor_fin_class_gen04_alias,
    a.payor_fin_class_gen03_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_financial_class_dim AS a
;
