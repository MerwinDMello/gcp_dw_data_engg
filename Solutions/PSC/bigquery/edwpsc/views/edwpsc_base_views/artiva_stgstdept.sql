CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgstdept`
AS SELECT
  `artiva_stgstdept`.stdeptid,
  `artiva_stgstdept`.psdeptdir,
  `artiva_stgstdept`.psdeptexec,
  `artiva_stgstdept`.psdeptmgr,
  `artiva_stgstdept`.stdeptdesc
  FROM
    edwpsc.`artiva_stgstdept`
;