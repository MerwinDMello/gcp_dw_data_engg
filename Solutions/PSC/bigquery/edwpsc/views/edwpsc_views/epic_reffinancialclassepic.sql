CREATE OR REPLACE VIEW edwpsc_views.`epic_reffinancialclassepic`
AS SELECT
  `epic_reffinancialclassepic`.financialclasskey,
  `epic_reffinancialclassepic`.financialclasscode,
  `epic_reffinancialclassepic`.financialclassname,
  `epic_reffinancialclassepic`.financialclassabbr,
  `epic_reffinancialclassepic`.finclassc,
  `epic_reffinancialclassepic`.regionkey,
  `epic_reffinancialclassepic`.sourceaprimarykey,
  `epic_reffinancialclassepic`.dwlastupdatedatetime,
  `epic_reffinancialclassepic`.sourcesystemcode,
  `epic_reffinancialclassepic`.insertedby,
  `epic_reffinancialclassepic`.inserteddtm,
  `epic_reffinancialclassepic`.modifiedby,
  `epic_reffinancialclassepic`.modifieddtm,
  `epic_reffinancialclassepic`.financialclassispatientbalance
  FROM
    edwpsc_base_views.`epic_reffinancialclassepic`
;