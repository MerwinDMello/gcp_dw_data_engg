-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_dollar_strf_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_dollar_strf_dim AS SELECT
    eis_dollar_strf_dim.dollar_strf_sid,
    eis_dollar_strf_dim.dollar_strf_member,
    eis_dollar_strf_dim.dollar_strf_alias,
    eis_dollar_strf_dim.dollar_strf_sort,
    eis_dollar_strf_dim.dollar_strf_gen02,
    eis_dollar_strf_dim.dollar_strf_gen02_alias,
    eis_dollar_strf_dim.dollar_strf_gen02_info,
    eis_dollar_strf_dim.dollar_strf_gen02_sort,
    eis_dollar_strf_dim.dollar_strf_gen03,
    eis_dollar_strf_dim.dollar_strf_gen03_alias,
    eis_dollar_strf_dim.dollar_strf_gen03_info,
    eis_dollar_strf_dim.dollar_strf_gen03_sort,
    eis_dollar_strf_dim.dollar_strf_gen04,
    eis_dollar_strf_dim.dollar_strf_gen04_alias,
    eis_dollar_strf_dim.dollar_strf_gen04_info,
    eis_dollar_strf_dim.dollar_strf_gen04_sort,
    eis_dollar_strf_dim.dollar_strf_gen05,
    eis_dollar_strf_dim.dollar_strf_gen05_alias,
    eis_dollar_strf_dim.dollar_strf_gen05_info,
    eis_dollar_strf_dim.dollar_strf_gen05_sort
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dollar_strf_dim
;
