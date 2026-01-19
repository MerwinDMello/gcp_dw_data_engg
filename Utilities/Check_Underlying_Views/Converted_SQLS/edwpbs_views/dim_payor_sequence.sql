-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_payor_sequence.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_payor_sequence AS SELECT
    a.payor_sequence_alias,
    a.payor_sequence_gen02,
    a.payor_sequence_gen02_alias,
    a.payor_sequence_gen02_info,
    a.payor_sequence_member,
    a.payor_sequence_sid,
    a.payor_sequence_sort,
    a.payor_sequence_gen02_sort
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_sequence_dim AS a
UNION ALL
SELECT
    'Patients' AS payor_sequence_alias,
    'PS_Patient' AS payor_sequence_gen02,
    'Payor Patients' AS payor_sequence_gen02_alias,
    substr(NULL, 1, 10) AS payor_sequence_gen02_info,
    'Pat' AS payor_sequence_member,
    0 AS payor_sequence_sid,
    CAST(NULL as INT64) AS payor_sequence_sort,
    0 AS payor_sequence_gen02_sort
  FROM
    (
      SELECT
          1 AS x
    ) AS dt
;
