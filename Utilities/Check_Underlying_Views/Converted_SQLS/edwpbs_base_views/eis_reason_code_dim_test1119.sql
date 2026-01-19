-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_reason_code_dim_test1119.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim_test1119 AS SELECT
    eis_reason_code_dim_test1119.reason_code_sid,
    eis_reason_code_dim_test1119.reason_code_member,
    eis_reason_code_dim_test1119.reason_code_alias,
    eis_reason_code_dim_test1119.reason_code_gen04,
    eis_reason_code_dim_test1119.reason_code_gen04_alias,
    eis_reason_code_dim_test1119.reason_code_gen03,
    eis_reason_code_dim_test1119.reason_code_gen03_alias,
    eis_reason_code_dim_test1119.reason_code_gen02,
    eis_reason_code_dim_test1119.reason_code_gen02_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.eis_reason_code_dim_test1119
;
