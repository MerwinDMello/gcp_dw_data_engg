CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsapprvltiers`
AS SELECT
  `artiva_stgpsapprvltiers`.psatid,
  `artiva_stgpsapprvltiers`.psatlevel,
  `artiva_stgpsapprvltiers`.psatmaximum,
  `artiva_stgpsapprvltiers`.psatminimum,
  `artiva_stgpsapprvltiers`.psatrole,
  `artiva_stgpsapprvltiers`.psattableid
  FROM
    edwpsc_base_views.`artiva_stgpsapprvltiers`
;