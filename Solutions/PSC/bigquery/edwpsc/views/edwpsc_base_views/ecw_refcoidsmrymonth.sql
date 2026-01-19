CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcoidsmrymonth`
AS SELECT
  `ecw_refcoidsmrymonth`.smrymonthkey,
  `ecw_refcoidsmrymonth`.snapshotdate,
  `ecw_refcoidsmrymonth`.displayorder,
  `ecw_refcoidsmrymonth`.displayname,
  `ecw_refcoidsmrymonth`.linksnapshotdate,
  `ecw_refcoidsmrymonth`.linkdatetype,
  `ecw_refcoidsmrymonth`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refcoidsmrymonth`
;