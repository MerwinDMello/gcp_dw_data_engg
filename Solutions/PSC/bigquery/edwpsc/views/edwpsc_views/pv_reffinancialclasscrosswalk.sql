CREATE OR REPLACE VIEW edwpsc_views.`pv_reffinancialclasscrosswalk`
AS SELECT
  `pv_reffinancialclasscrosswalk`.financialclasskey,
  `pv_reffinancialclasscrosswalk`.sourcefinancialclass,
  `pv_reffinancialclasscrosswalk`.ispatientflag,
  `pv_reffinancialclasscrosswalk`.dwlastupdatedatetime,
  `pv_reffinancialclasscrosswalk`.sourcesystemcode
  FROM
    edwpsc_base_views.`pv_reffinancialclasscrosswalk`
;