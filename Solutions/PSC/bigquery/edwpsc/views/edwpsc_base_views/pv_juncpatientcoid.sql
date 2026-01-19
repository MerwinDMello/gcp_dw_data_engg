CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncpatientcoid`
AS SELECT
  `pv_juncpatientcoid`.juncpatientcoidkey,
  `pv_juncpatientcoid`.patientkey,
  `pv_juncpatientcoid`.coid,
  `pv_juncpatientcoid`.insertedby,
  `pv_juncpatientcoid`.inserteddtm,
  `pv_juncpatientcoid`.modifiedby,
  `pv_juncpatientcoid`.modifieddtm,
  `pv_juncpatientcoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_juncpatientcoid`
;