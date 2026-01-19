-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_reason_code_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_reason_code_dim AS SELECT
    ROUND(eis_reason_code_dim.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
    eis_reason_code_dim.reason_code_member,
    eis_reason_code_dim.reason_code_alias,
    eis_reason_code_dim.reason_code_sort,
    eis_reason_code_dim.reason_code_gen02,
    eis_reason_code_dim.reason_code_gen02_alias,
    eis_reason_code_dim.reason_code_gen02_info,
    eis_reason_code_dim.reason_code_gen02_sort,
    eis_reason_code_dim.reason_code_gen03,
    eis_reason_code_dim.reason_code_gen03_alias,
    eis_reason_code_dim.reason_code_gen03_info,
    eis_reason_code_dim.reason_code_gen03_sort,
    eis_reason_code_dim.reason_code_gen04,
    eis_reason_code_dim.reason_code_gen04_alias,
    eis_reason_code_dim.reason_code_gen04_info,
    eis_reason_code_dim.reason_code_gen04_sort,
    eis_reason_code_dim.reason_code_gen05,
    eis_reason_code_dim.reason_code_gen05_alias,
    eis_reason_code_dim.reason_code_gen05_info,
    eis_reason_code_dim.reason_code_gen05_sort,
    eis_reason_code_dim.reason_code_gen06,
    eis_reason_code_dim.reason_code_gen06_alias,
    eis_reason_code_dim.reason_code_gen06_info,
    eis_reason_code_dim.reason_code_gen06_sort
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_reason_code_dim
;
