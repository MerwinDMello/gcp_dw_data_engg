-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_payor_financial_class_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_financial_class_dim AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS a
;
