CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcenter`
AS SELECT
  `ecw_refcenter`.centerkey,
  `ecw_refcenter`.centerdescription,
  `ecw_refcenter`.dwlastupdatedatetime,
  `ecw_refcenter`.sourcesystemcode,
  `ecw_refcenter`.insertedby,
  `ecw_refcenter`.inserteddtm,
  `ecw_refcenter`.modifiedby,
  `ecw_refcenter`.modifieddtm,
  `ecw_refcenter`.sysstarttime,
  `ecw_refcenter`.sysendtime
  FROM
    edwpsc.`ecw_refcenter`
;