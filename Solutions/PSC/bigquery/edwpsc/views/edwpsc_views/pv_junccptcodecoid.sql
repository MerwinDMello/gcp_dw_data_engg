CREATE OR REPLACE VIEW edwpsc_views.`pv_junccptcodecoid`
AS SELECT
  `pv_junccptcodecoid`.junccptcodecoidkey,
  `pv_junccptcodecoid`.cptcodekey,
  `pv_junccptcodecoid`.coid,
  `pv_junccptcodecoid`.insertedby,
  `pv_junccptcodecoid`.inserteddtm,
  `pv_junccptcodecoid`.modifiedby,
  `pv_junccptcodecoid`.modifieddtm,
  `pv_junccptcodecoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_junccptcodecoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_junccptcodecoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;