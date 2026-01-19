CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refstate`
AS SELECT
  `epic_refstate`.statekey,
  `epic_refstate`.stateabbr,
  `epic_refstate`.statename,
  `epic_refstate`.statec,
  `epic_refstate`.sourceaprimarykey,
  `epic_refstate`.dwlastupdatedatetime,
  `epic_refstate`.sourcesystemcode,
  `epic_refstate`.insertedby,
  `epic_refstate`.inserteddtm,
  `epic_refstate`.modifiedby,
  `epic_refstate`.modifieddtm
  FROM
    edwpsc.`epic_refstate`
;