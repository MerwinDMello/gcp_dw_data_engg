CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsapprvltables`
AS SELECT
  `artiva_stgpsapprvltables`.pstadesc,
  `artiva_stgpsapprvltables`.pstaname,
  `artiva_stgpsapprvltables`.pstarequestrole
  FROM
    edwpsc.`artiva_stgpsapprvltables`
;