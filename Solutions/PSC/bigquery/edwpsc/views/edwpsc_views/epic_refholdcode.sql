CREATE OR REPLACE VIEW edwpsc_views.`epic_refholdcode`
AS SELECT
  `epic_refholdcode`.holdcodekey,
  `epic_refholdcode`.holdcode,
  `epic_refholdcode`.holdcodename,
  `epic_refholdcode`.holdcodetype,
  `epic_refholdcode`.deleteflag,
  `epic_refholdcode`.holdcodedescription,
  `epic_refholdcode`.regionkey,
  `epic_refholdcode`.sourceaprimarykeyvalue,
  `epic_refholdcode`.sourcearecordlastupdated,
  `epic_refholdcode`.dwlastupdatedatetime,
  `epic_refholdcode`.sourcesystemcode,
  `epic_refholdcode`.insertedby,
  `epic_refholdcode`.inserteddtm,
  `epic_refholdcode`.modifiedby,
  `epic_refholdcode`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refholdcode`
;