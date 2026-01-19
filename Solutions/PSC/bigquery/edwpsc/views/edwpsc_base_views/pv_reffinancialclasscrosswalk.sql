CREATE OR REPLACE VIEW edwpsc_base_views.`pv_reffinancialclasscrosswalk`
AS SELECT
  `pv_reffinancialclasscrosswalk`.financialclasskey,
  `pv_reffinancialclasscrosswalk`.sourcefinancialclass,
  `pv_reffinancialclasscrosswalk`.ispatientflag,
  `pv_reffinancialclasscrosswalk`.dwlastupdatedatetime,
  `pv_reffinancialclasscrosswalk`.sourcesystemcode
  FROM
    edwpsc.`pv_reffinancialclasscrosswalk`
;