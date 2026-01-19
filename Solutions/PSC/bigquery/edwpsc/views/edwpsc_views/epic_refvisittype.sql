CREATE OR REPLACE VIEW edwpsc_views.`epic_refvisittype`
AS SELECT
  `epic_refvisittype`.visittypekey,
  `epic_refvisittype`.visittypename,
  `epic_refvisittype`.visittypeabbr,
  `epic_refvisittype`.visittypedescription,
  `epic_refvisittype`.visittyperequiredclaim,
  `epic_refvisittype`.visittyperequiredcopay,
  `epic_refvisittype`.visittypepregnancyvisit,
  `epic_refvisittype`.visittypeactiveflag,
  `epic_refvisittype`.visittypeorthovisit,
  `epic_refvisittype`.visittypeobgynvisit,
  `epic_refvisittype`.visittypeisvisit,
  `epic_refvisittype`.visittypewebvisit,
  `epic_refvisittype`.visittypephysicaltherapyvisit,
  `epic_refvisittype`.sourceprimarykeyvalue,
  `epic_refvisittype`.dwlastupdatedatetime,
  `epic_refvisittype`.sourcesystemcode,
  `epic_refvisittype`.insertedby,
  `epic_refvisittype`.inserteddtm,
  `epic_refvisittype`.modifiedby,
  `epic_refvisittype`.modifieddtm,
  `epic_refvisittype`.deleteflag,
  `epic_refvisittype`.regionkey
  FROM
    edwpsc_base_views.`epic_refvisittype`
;