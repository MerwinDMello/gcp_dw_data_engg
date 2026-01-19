CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncpracticecoid`
AS SELECT
  `epic_juncpracticecoid`.juncpracticecoid,
  `epic_juncpracticecoid`.practicekey,
  `epic_juncpracticecoid`.coid,
  `epic_juncpracticecoid`.insertedby,
  `epic_juncpracticecoid`.inserteddtm,
  `epic_juncpracticecoid`.modifiedby,
  `epic_juncpracticecoid`.modifieddtm,
  `epic_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncpracticecoid`
;