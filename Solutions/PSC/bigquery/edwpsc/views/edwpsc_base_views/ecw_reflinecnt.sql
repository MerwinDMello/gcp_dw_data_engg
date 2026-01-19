CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reflinecnt`
AS SELECT
  `ecw_reflinecnt`.linecnt,
  `ecw_reflinecnt`.linedesc,
  `ecw_reflinecnt`.linesubgroup,
  `ecw_reflinecnt`.sourcearecordlastupdated,
  `ecw_reflinecnt`.sourcebrecordlastupdated,
  `ecw_reflinecnt`.dwlastupdatedatetime,
  `ecw_reflinecnt`.sourcesystemcode,
  `ecw_reflinecnt`.linesubgrouporder
  FROM
    edwpsc.`ecw_reflinecnt`
;