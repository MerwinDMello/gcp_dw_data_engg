CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncpracticecoid`
AS SELECT
  `ecw_juncpracticecoid`.juncpracticecoid,
  `ecw_juncpracticecoid`.practicekey,
  `ecw_juncpracticecoid`.coid,
  `ecw_juncpracticecoid`.insertedby,
  `ecw_juncpracticecoid`.inserteddtm,
  `ecw_juncpracticecoid`.modifiedby,
  `ecw_juncpracticecoid`.modifieddtm,
  `ecw_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncpracticecoid`
;