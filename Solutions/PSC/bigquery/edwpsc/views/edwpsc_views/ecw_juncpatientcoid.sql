CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncpatientcoid`
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
    edwpsc_base_views.`ecw_juncpatientcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncpatientcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;