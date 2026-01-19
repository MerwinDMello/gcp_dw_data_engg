CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncdivisioncoid`
AS SELECT
  `ecw_juncdivisioncoid`.juncdivisioncoidkey,
  `ecw_juncdivisioncoid`.divisionkey,
  `ecw_juncdivisioncoid`.coid,
  `ecw_juncdivisioncoid`.insertedby,
  `ecw_juncdivisioncoid`.inserteddtm,
  `ecw_juncdivisioncoid`.modifiedby,
  `ecw_juncdivisioncoid`.modifieddtm,
  `ecw_juncdivisioncoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncdivisioncoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncdivisioncoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;