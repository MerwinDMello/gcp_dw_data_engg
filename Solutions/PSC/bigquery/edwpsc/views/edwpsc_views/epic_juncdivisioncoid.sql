CREATE OR REPLACE VIEW edwpsc_views.`epic_juncdivisioncoid`
AS SELECT
  `epic_juncdivisioncoid`.juncdivisioncoidkey,
  `epic_juncdivisioncoid`.divisionkey,
  `epic_juncdivisioncoid`.coid,
  `epic_juncdivisioncoid`.insertedby,
  `epic_juncdivisioncoid`.inserteddtm,
  `epic_juncdivisioncoid`.modifiedby,
  `epic_juncdivisioncoid`.modifieddtm,
  `epic_juncdivisioncoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_juncdivisioncoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncdivisioncoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;