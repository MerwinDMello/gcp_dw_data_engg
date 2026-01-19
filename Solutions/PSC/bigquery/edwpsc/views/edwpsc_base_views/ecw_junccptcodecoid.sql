CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_junccptcodecoid`
AS SELECT
  `ecw_junccptcodecoid`.junccptcodecoidkey,
  `ecw_junccptcodecoid`.cptcodekey,
  `ecw_junccptcodecoid`.coid,
  `ecw_junccptcodecoid`.insertedby,
  `ecw_junccptcodecoid`.inserteddtm,
  `ecw_junccptcodecoid`.modifiedby,
  `ecw_junccptcodecoid`.modifieddtm,
  `ecw_junccptcodecoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_junccptcodecoid`
;