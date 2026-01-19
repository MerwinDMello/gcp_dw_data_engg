CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcenter_history`
AS SELECT
  `ecw_refcenter_history`.centerkey,
  `ecw_refcenter_history`.centerdescription,
  `ecw_refcenter_history`.dwlastupdatedatetime,
  `ecw_refcenter_history`.sourcesystemcode,
  `ecw_refcenter_history`.insertedby,
  `ecw_refcenter_history`.inserteddtm,
  `ecw_refcenter_history`.modifiedby,
  `ecw_refcenter_history`.modifieddtm,
  `ecw_refcenter_history`.sysstarttime,
  `ecw_refcenter_history`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refcenter_history`
;