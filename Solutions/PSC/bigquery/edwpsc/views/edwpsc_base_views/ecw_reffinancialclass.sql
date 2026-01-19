CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reffinancialclass`
AS SELECT
  `ecw_reffinancialclass`.financialclasskey,
  `ecw_reffinancialclass`.financialclassname,
  `ecw_reffinancialclass`.financialclassispatientbalance,
  `ecw_reffinancialclass`.dwlastupdatedatetime,
  `ecw_reffinancialclass`.sourcesystemcode,
  `ecw_reffinancialclass`.insertedby,
  `ecw_reffinancialclass`.inserteddtm,
  `ecw_reffinancialclass`.modifiedby,
  `ecw_reffinancialclass`.modifieddtm
  FROM
    edwpsc.`ecw_reffinancialclass`
;