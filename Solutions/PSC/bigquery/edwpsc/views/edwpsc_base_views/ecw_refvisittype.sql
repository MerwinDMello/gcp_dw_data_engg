CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refvisittype`
AS SELECT
  `ecw_refvisittype`.visittypekey,
  `ecw_refvisittype`.visittypename,
  `ecw_refvisittype`.visittypedescription,
  `ecw_refvisittype`.visittyperequiredclaim,
  `ecw_refvisittype`.visittyperequiredcopay,
  `ecw_refvisittype`.visittypepregnancyvisit,
  `ecw_refvisittype`.visittypeactiveflag,
  `ecw_refvisittype`.visittypeorthovisit,
  `ecw_refvisittype`.visittypeobgynvisit,
  `ecw_refvisittype`.visittypeisvisit,
  `ecw_refvisittype`.visittypewebvisit,
  `ecw_refvisittype`.visittypephysicaltherapyvisit,
  `ecw_refvisittype`.sourceprimarykeyvalue,
  `ecw_refvisittype`.sourcerecordlastupdated,
  `ecw_refvisittype`.dwlastupdatedatetime,
  `ecw_refvisittype`.sourcesystemcode,
  `ecw_refvisittype`.insertedby,
  `ecw_refvisittype`.inserteddtm,
  `ecw_refvisittype`.modifiedby,
  `ecw_refvisittype`.modifieddtm,
  `ecw_refvisittype`.deleteflag,
  `ecw_refvisittype`.sysstarttime,
  `ecw_refvisittype`.sysendtime
  FROM
    edwpsc.`ecw_refvisittype`
;