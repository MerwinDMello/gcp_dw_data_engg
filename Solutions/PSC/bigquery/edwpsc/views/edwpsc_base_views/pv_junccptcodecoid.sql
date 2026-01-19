CREATE OR REPLACE VIEW edwpsc_base_views.`pv_junccptcodecoid`
AS SELECT
  `pv_junccptcodecoid`.junccptcodecoidkey,
  `pv_junccptcodecoid`.cptcodekey,
  `pv_junccptcodecoid`.coid,
  `pv_junccptcodecoid`.insertedby,
  `pv_junccptcodecoid`.inserteddtm,
  `pv_junccptcodecoid`.modifiedby,
  `pv_junccptcodecoid`.modifieddtm,
  `pv_junccptcodecoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_junccptcodecoid`
;