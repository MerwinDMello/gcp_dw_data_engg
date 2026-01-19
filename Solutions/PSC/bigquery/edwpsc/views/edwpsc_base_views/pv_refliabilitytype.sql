CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refliabilitytype`
AS SELECT
  `pv_refliabilitytype`.liabilityownertype,
  `pv_refliabilitytype`.liabilityownerdesc,
  `pv_refliabilitytype`.liabilityownerdesc2,
  `pv_refliabilitytype`.dwlastupdatedatetime,
  `pv_refliabilitytype`.sourcesystemcode,
  `pv_refliabilitytype`.insertedby,
  `pv_refliabilitytype`.inserteddtm,
  `pv_refliabilitytype`.modifiedby,
  `pv_refliabilitytype`.modifieddtm
  FROM
    edwpsc.`pv_refliabilitytype`
;