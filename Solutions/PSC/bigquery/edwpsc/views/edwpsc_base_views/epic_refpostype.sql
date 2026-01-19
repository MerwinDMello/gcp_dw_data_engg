CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refpostype`
AS SELECT
  `epic_refpostype`.postypekey,
  `epic_refpostype`.postypename,
  `epic_refpostype`.postypetitle,
  `epic_refpostype`.postypeabbr,
  `epic_refpostype`.postypeinternalid,
  `epic_refpostype`.postypec,
  `epic_refpostype`.regionkey,
  `epic_refpostype`.sourceaprimarykey,
  `epic_refpostype`.dwlastupdatedatetime,
  `epic_refpostype`.sourcesystemcode,
  `epic_refpostype`.insertedby,
  `epic_refpostype`.inserteddtm,
  `epic_refpostype`.modifiedby,
  `epic_refpostype`.modifieddtm
  FROM
    edwpsc.`epic_refpostype`
;