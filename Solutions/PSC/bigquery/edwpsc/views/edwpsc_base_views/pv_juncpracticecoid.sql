CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncpracticecoid`
AS SELECT
  `pv_juncpracticecoid`.juncpracticecoid,
  `pv_juncpracticecoid`.practicekey,
  `pv_juncpracticecoid`.coid,
  `pv_juncpracticecoid`.insertedby,
  `pv_juncpracticecoid`.inserteddtm,
  `pv_juncpracticecoid`.modifiedby,
  `pv_juncpracticecoid`.modifieddtm,
  `pv_juncpracticecoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_juncpracticecoid`
;