CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refliabilitytype`
AS SELECT
  `ecw_refliabilitytype`.liabilityownertype,
  `ecw_refliabilitytype`.liabilityownerdesc,
  `ecw_refliabilitytype`.liabilityownerdesc2,
  `ecw_refliabilitytype`.dwlastupdatedatetime,
  `ecw_refliabilitytype`.sourcesystemcode,
  `ecw_refliabilitytype`.insertedby,
  `ecw_refliabilitytype`.inserteddtm,
  `ecw_refliabilitytype`.modifiedby,
  `ecw_refliabilitytype`.modifieddtm
  FROM
    edwpsc.`ecw_refliabilitytype`
;