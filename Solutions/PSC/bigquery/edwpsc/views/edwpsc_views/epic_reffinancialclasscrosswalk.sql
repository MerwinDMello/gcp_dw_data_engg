CREATE OR REPLACE VIEW edwpsc_views.`epic_reffinancialclasscrosswalk`
AS SELECT
  `epic_reffinancialclasscrosswalk`.sourcesystemfinclassc,
  `epic_reffinancialclasscrosswalk`.financialclasscode,
  `epic_reffinancialclasscrosswalk`.regionkey
  FROM
    edwpsc_base_views.`epic_reffinancialclasscrosswalk`
;