CREATE OR REPLACE VIEW edwpsc_views.`epic_refbillarea`
AS SELECT
  `epic_refbillarea`.billareakey,
  `epic_refbillarea`.billareaname,
  `epic_refbillarea`.billareaabbr,
  `epic_refbillarea`.billareaglprefix,
  `epic_refbillarea`.billareaid,
  `epic_refbillarea`.regionkey,
  `epic_refbillarea`.sourceaprimarykey,
  `epic_refbillarea`.dwlastupdatedatetime,
  `epic_refbillarea`.sourcesystemcode,
  `epic_refbillarea`.insertedby,
  `epic_refbillarea`.inserteddtm,
  `epic_refbillarea`.modifiedby,
  `epic_refbillarea`.modifieddtm,
  `epic_refbillarea`.billareafederaltaxid
  FROM
    edwpsc_base_views.`epic_refbillarea`
;