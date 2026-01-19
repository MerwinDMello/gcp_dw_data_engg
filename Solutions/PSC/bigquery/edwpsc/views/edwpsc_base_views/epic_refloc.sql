CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refloc`
AS SELECT
  `epic_refloc`.lockey,
  `epic_refloc`.locname,
  `epic_refloc`.locabbr,
  `epic_refloc`.locglprefix,
  `epic_refloc`.locservareaid,
  `epic_refloc`.serviceareakey,
  `epic_refloc`.deleteflag,
  `epic_refloc`.locpostype,
  `epic_refloc`.locposcode,
  `epic_refloc`.locfacilityid,
  `epic_refloc`.locid,
  `epic_refloc`.regionkey,
  `epic_refloc`.sourceaprimarykey,
  `epic_refloc`.dwlastupdatedatetime,
  `epic_refloc`.sourcesystemcode,
  `epic_refloc`.insertedby,
  `epic_refloc`.inserteddtm,
  `epic_refloc`.modifiedby,
  `epic_refloc`.modifieddtm
  FROM
    edwpsc.`epic_refloc`
;