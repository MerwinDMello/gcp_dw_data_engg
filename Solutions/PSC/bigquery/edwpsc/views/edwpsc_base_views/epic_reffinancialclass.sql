CREATE OR REPLACE VIEW edwpsc_base_views.`epic_reffinancialclass`
AS SELECT
  `epic_reffinancialclass`.financialclasskey,
  `epic_reffinancialclass`.financialclassname,
  `epic_reffinancialclass`.financialclassispatientbalance,
  `epic_reffinancialclass`.dwlastupdatedatetime,
  `epic_reffinancialclass`.sourcesystemcode,
  `epic_reffinancialclass`.insertedby,
  `epic_reffinancialclass`.inserteddtm,
  `epic_reffinancialclass`.modifiedby,
  `epic_reffinancialclass`.modifieddtm
  FROM
    edwpsc.`epic_reffinancialclass`
;