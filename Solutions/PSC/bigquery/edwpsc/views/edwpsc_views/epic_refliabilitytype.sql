CREATE OR REPLACE VIEW edwpsc_views.`epic_refliabilitytype`
AS SELECT
  `epic_refliabilitytype`.liabilityownertype,
  `epic_refliabilitytype`.liabilityownerdesc,
  `epic_refliabilitytype`.liabilityownerdesc2,
  `epic_refliabilitytype`.dwlastupdatedatetime,
  `epic_refliabilitytype`.sourcesystemcode,
  `epic_refliabilitytype`.insertedby,
  `epic_refliabilitytype`.inserteddtm,
  `epic_refliabilitytype`.modifiedby,
  `epic_refliabilitytype`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refliabilitytype`
;