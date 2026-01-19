CREATE OR REPLACE VIEW edwpsc_views.`pv_refvisittype`
AS SELECT
  `pv_refvisittype`.visittypekey,
  `pv_refvisittype`.visittypename,
  `pv_refvisittype`.visittypedescription,
  `pv_refvisittype`.visittyperequiredclaim,
  `pv_refvisittype`.visittyperequiredcopay,
  `pv_refvisittype`.visittypepregnancyvisit,
  `pv_refvisittype`.visittypeactiveflag,
  `pv_refvisittype`.visittypeorthovisit,
  `pv_refvisittype`.visittypeobgynvisit,
  `pv_refvisittype`.visittypeisvisit,
  `pv_refvisittype`.visittypewebvisit,
  `pv_refvisittype`.visittypephysicaltherapyvisit,
  `pv_refvisittype`.sourceprimarykeyvalue,
  `pv_refvisittype`.sourcerecordlastupdated,
  `pv_refvisittype`.dwlastupdatedatetime,
  `pv_refvisittype`.sourcesystemcode,
  `pv_refvisittype`.insertedby,
  `pv_refvisittype`.inserteddtm,
  `pv_refvisittype`.modifiedby,
  `pv_refvisittype`.modifieddtm,
  `pv_refvisittype`.deleteflag
  FROM
    edwpsc_base_views.`pv_refvisittype`
;