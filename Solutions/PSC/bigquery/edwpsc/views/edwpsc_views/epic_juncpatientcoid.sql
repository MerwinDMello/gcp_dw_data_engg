CREATE OR REPLACE VIEW edwpsc_views.`epic_juncpatientcoid`
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
    edwpsc_base_views.`epic_juncpatientcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncpatientcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;