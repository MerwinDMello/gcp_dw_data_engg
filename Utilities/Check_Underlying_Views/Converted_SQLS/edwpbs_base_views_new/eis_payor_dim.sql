-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_payor_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS SELECT
    eis_payor_dim.payor_sid,
    eis_payor_dim.payor_member,
    eis_payor_dim.payor_alias,
    eis_payor_dim.payor_gen02,
    eis_payor_dim.payor_gen02_sort_id,
    eis_payor_dim.payor_gen02_info,
    eis_payor_dim.payor_gen02_alias,
    eis_payor_dim.payor_gen03,
    eis_payor_dim.payor_gen03_sort_id,
    eis_payor_dim.payor_gen03_info,
    eis_payor_dim.payor_gen03_alias,
    eis_payor_dim.payor_gen04,
    eis_payor_dim.payor_gen04_sort_id,
    eis_payor_dim.payor_gen04_info,
    eis_payor_dim.payor_gen04_alias,
    eis_payor_dim.payor_gen05,
    eis_payor_dim.payor_gen05_sort_id,
    eis_payor_dim.payor_gen05_info,
    eis_payor_dim.payor_gen05_alias,
    eis_payor_dim.data_exists_flag01,
    eis_payor_dim.data_exists_flag02,
    eis_payor_dim.data_exists_flag03,
    eis_payor_dim.data_exists_flag04,
    eis_payor_dim.data_exists_flag05,
    eis_payor_dim.data_exists_flag06,
    eis_payor_dim.data_exists_flag07,
    eis_payor_dim.data_exists_flag08,
    eis_payor_dim.data_exists_flag09,
    eis_payor_dim.data_exists_flag10
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.eis_payor_dim
;
