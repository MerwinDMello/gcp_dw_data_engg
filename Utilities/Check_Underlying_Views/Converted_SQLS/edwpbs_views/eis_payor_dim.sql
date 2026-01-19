-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_payor_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_payor_dim
   OPTIONS(description='Copy of Payor Dimension from EDWPF.')
  AS SELECT
      a.payor_sid,
      a.payor_member,
      a.payor_alias,
      a.payor_gen02,
      a.payor_gen02_sort_id,
      a.payor_gen02_info,
      a.payor_gen02_alias,
      a.payor_gen03,
      a.payor_gen03_sort_id,
      a.payor_gen03_info,
      a.payor_gen03_alias,
      a.payor_gen04,
      a.payor_gen04_sort_id,
      a.payor_gen04_info,
      a.payor_gen04_alias,
      a.payor_gen05,
      a.payor_gen05_sort_id,
      a.payor_gen05_info,
      a.payor_gen05_alias,
      a.data_exists_flag01,
      a.data_exists_flag02,
      a.data_exists_flag03,
      a.data_exists_flag04,
      a.data_exists_flag05,
      a.data_exists_flag06,
      a.data_exists_flag07,
      a.data_exists_flag08,
      a.data_exists_flag09,
      a.data_exists_flag10
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS a
  ;
