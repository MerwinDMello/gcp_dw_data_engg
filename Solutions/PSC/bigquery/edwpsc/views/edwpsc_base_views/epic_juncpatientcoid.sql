CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncpatientcoid`
AS SELECT
  `epic_juncpatientcoid`.juncpatientcoidkey,
  `epic_juncpatientcoid`.patientkey,
  `epic_juncpatientcoid`.coid,
  `epic_juncpatientcoid`.insertedby,
  `epic_juncpatientcoid`.inserteddtm,
  `epic_juncpatientcoid`.modifiedby,
  `epic_juncpatientcoid`.modifieddtm,
  `epic_juncpatientcoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncpatientcoid`
;