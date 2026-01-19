CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncpatientcoid`
AS SELECT
  `ecw_juncpatientcoid`.juncpatientcoidkey,
  `ecw_juncpatientcoid`.patientkey,
  `ecw_juncpatientcoid`.coid,
  `ecw_juncpatientcoid`.insertedby,
  `ecw_juncpatientcoid`.inserteddtm,
  `ecw_juncpatientcoid`.modifiedby,
  `ecw_juncpatientcoid`.modifieddtm,
  `ecw_juncpatientcoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncpatientcoid`
;