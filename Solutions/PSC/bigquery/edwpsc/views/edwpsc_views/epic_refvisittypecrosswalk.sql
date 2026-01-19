CREATE OR REPLACE VIEW edwpsc_views.`epic_refvisittypecrosswalk`
AS SELECT
  `epic_refvisittypecrosswalk`.sourceaprimarykeyvalue,
  `epic_refvisittypecrosswalk`.dispenctypename,
  `epic_refvisittypecrosswalk`.visittyperequiredclaim,
  `epic_refvisittypecrosswalk`.visittyperequiredcopay,
  `epic_refvisittypecrosswalk`.visittypepregnancyvisit,
  `epic_refvisittypecrosswalk`.visittypeactiveflag,
  `epic_refvisittypecrosswalk`.visittypeorthovisit,
  `epic_refvisittypecrosswalk`.visittypeobgynvisit,
  `epic_refvisittypecrosswalk`.visittypeisvisit,
  `epic_refvisittypecrosswalk`.visittypewebvisit,
  `epic_refvisittypecrosswalk`.visittypephysicaltherapyvisit
  FROM
    edwpsc_base_views.`epic_refvisittypecrosswalk`
;