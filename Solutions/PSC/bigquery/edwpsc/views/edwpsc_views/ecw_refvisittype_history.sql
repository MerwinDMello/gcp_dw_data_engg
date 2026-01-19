CREATE OR REPLACE VIEW edwpsc_views.`ecw_refvisittype_history`
AS SELECT
  `ecw_refvisittype_history`.visittypekey,
  `ecw_refvisittype_history`.visittypename,
  `ecw_refvisittype_history`.visittypedescription,
  `ecw_refvisittype_history`.visittyperequiredclaim,
  `ecw_refvisittype_history`.visittyperequiredcopay,
  `ecw_refvisittype_history`.visittypepregnancyvisit,
  `ecw_refvisittype_history`.visittypeactiveflag,
  `ecw_refvisittype_history`.visittypeorthovisit,
  `ecw_refvisittype_history`.visittypeobgynvisit,
  `ecw_refvisittype_history`.visittypeisvisit,
  `ecw_refvisittype_history`.visittypewebvisit,
  `ecw_refvisittype_history`.visittypephysicaltherapyvisit,
  `ecw_refvisittype_history`.sourceprimarykeyvalue,
  `ecw_refvisittype_history`.sourcerecordlastupdated,
  `ecw_refvisittype_history`.dwlastupdatedatetime,
  `ecw_refvisittype_history`.sourcesystemcode,
  `ecw_refvisittype_history`.insertedby,
  `ecw_refvisittype_history`.inserteddtm,
  `ecw_refvisittype_history`.modifiedby,
  `ecw_refvisittype_history`.modifieddtm,
  `ecw_refvisittype_history`.deleteflag,
  `ecw_refvisittype_history`.sysstarttime,
  `ecw_refvisittype_history`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refvisittype_history`
;