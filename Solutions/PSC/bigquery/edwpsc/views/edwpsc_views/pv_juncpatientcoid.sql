CREATE OR REPLACE VIEW edwpsc_views.`pv_juncpatientcoid`
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
    edwpsc_base_views.`pv_juncpatientcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncpatientcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;