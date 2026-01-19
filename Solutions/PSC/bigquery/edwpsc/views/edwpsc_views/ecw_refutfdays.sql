CREATE OR REPLACE VIEW edwpsc_views.`ecw_refutfdays`
AS SELECT
  `ecw_refutfdays`.iplankey,
  `ecw_refutfdays`.coidstate,
  `ecw_refutfdays`.iplanid,
  `ecw_refutfdays`.appealdays,
  `ecw_refutfdays`.initialsubmissiondays,
  `ecw_refutfdays`.appealtype,
  `ecw_refutfdays`.dwlastupdatedatetime,
  `ecw_refutfdays`.sourcesystemcode,
  `ecw_refutfdays`.insertedby,
  `ecw_refutfdays`.inserteddtm,
  `ecw_refutfdays`.modifiedby,
  `ecw_refutfdays`.modifieddtm,
  `ecw_refutfdays`.lastverificationdate
  FROM
    edwpsc_base_views.`ecw_refutfdays`
;