CREATE OR REPLACE VIEW edwpsc_views.`ecw_junccptcodecoid`
AS SELECT
  `ecw_junccptcodecoid`.junccptcodecoidkey,
  `ecw_junccptcodecoid`.cptcodekey,
  `ecw_junccptcodecoid`.coid,
  `ecw_junccptcodecoid`.insertedby,
  `ecw_junccptcodecoid`.inserteddtm,
  `ecw_junccptcodecoid`.modifiedby,
  `ecw_junccptcodecoid`.modifieddtm,
  `ecw_junccptcodecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_junccptcodecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_junccptcodecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;