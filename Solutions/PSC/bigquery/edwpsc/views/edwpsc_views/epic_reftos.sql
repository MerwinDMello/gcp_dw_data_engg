CREATE OR REPLACE VIEW edwpsc_views.`epic_reftos`
AS SELECT
  `epic_reftos`.toskey,
  `epic_reftos`.tosname,
  `epic_reftos`.tostitle,
  `epic_reftos`.tosabbr,
  `epic_reftos`.tosinternalid,
  `epic_reftos`.tosc,
  `epic_reftos`.regionkey,
  `epic_reftos`.sourceaprimarykey,
  `epic_reftos`.dwlastupdatedatetime,
  `epic_reftos`.sourcesystemcode,
  `epic_reftos`.insertedby,
  `epic_reftos`.inserteddtm,
  `epic_reftos`.modifiedby,
  `epic_reftos`.modifieddtm
  FROM
    edwpsc_base_views.`epic_reftos`
;