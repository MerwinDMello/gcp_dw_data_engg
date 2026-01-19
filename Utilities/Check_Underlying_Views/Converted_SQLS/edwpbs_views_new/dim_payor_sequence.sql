-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
