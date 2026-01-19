-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_age_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_age_dim AS SELECT
    eis_age_dim.age_month_sid,
    eis_age_dim.age_member,
    eis_age_dim.age_alias,
    eis_age_dim.age_gen02,
    eis_age_dim.age_gen02_alias,
    eis_age_dim.age_gen02_info,
    eis_age_dim.age_gen03,
    eis_age_dim.age_gen03_alias,
    eis_age_dim.age_gen03_info,
    eis_age_dim.age_gen04,
    eis_age_dim.age_gen04_alias,
    eis_age_dim.age_gen04_info,
    eis_age_dim.age_gen05,
    eis_age_dim.age_gen05_alias,
    eis_age_dim.age_gen06,
    eis_age_dim.age_gen06_alias,
    eis_age_dim.age_gen07,
    eis_age_dim.age_gen07_alias,
    eis_age_dim.age_gen08,
    eis_age_dim.age_gen08_alias,
    eis_age_dim.age_gen09,
    eis_age_dim.age_gen09_alias,
    eis_age_dim.age_gen10,
    eis_age_dim.age_gen10_alias,
    eis_age_dim.age_gen11,
    eis_age_dim.age_gen11_alias,
    eis_age_dim.age_gen12,
    eis_age_dim.age_gen12_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_age_dim
;
