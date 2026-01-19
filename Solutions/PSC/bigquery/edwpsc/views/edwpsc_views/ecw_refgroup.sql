CREATE OR REPLACE VIEW edwpsc_views.`ecw_refgroup`
AS SELECT
  `ecw_refgroup`.groupkey,
  `ecw_refgroup`.groupname,
  `ecw_refgroup`.groupisvisibleonfactclaim,
  `ecw_refgroup`.sourceprimarykeyvalue,
  `ecw_refgroup`.sourcerecordlastupdated,
  `ecw_refgroup`.dwlastupdatedatetime,
  `ecw_refgroup`.sourcesystemcode,
  `ecw_refgroup`.insertedby,
  `ecw_refgroup`.inserteddtm,
  `ecw_refgroup`.modifiedby,
  `ecw_refgroup`.modifieddtm,
  `ecw_refgroup`.groupcode,
  `ecw_refgroup`.deleteflag,
  `ecw_refgroup`.coidstatflag,
  `ecw_refgroup`.ppmsflag,
  `ecw_refgroup`.sysstarttime,
  `ecw_refgroup`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refgroup`
;