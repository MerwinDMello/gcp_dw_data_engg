-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_dollar_strf_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_dollar_strf_dim AS SELECT
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
    `hca-hin-curated-mirroring-td`.edwpbs.eis_dollar_strf_dim
;
