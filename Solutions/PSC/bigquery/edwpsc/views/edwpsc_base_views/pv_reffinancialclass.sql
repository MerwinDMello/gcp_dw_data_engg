CREATE OR REPLACE VIEW edwpsc_base_views.`pv_reffinancialclass`
AS SELECT
  `pv_reffinancialclass`.financialclasskey,
  `pv_reffinancialclass`.financialclassname,
  `pv_reffinancialclass`.financialclassispatientbalance,
  `pv_reffinancialclass`.dwlastupdatedatetime,
  `pv_reffinancialclass`.sourcesystemcode,
  `pv_reffinancialclass`.insertedby,
  `pv_reffinancialclass`.inserteddtm,
  `pv_reffinancialclass`.modifiedby,
  `pv_reffinancialclass`.modifieddtm
  FROM
    edwpsc.`pv_reffinancialclass`
;