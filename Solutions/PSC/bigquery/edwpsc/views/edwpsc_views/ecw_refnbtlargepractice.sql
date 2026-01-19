CREATE OR REPLACE VIEW edwpsc_views.`ecw_refnbtlargepractice`
AS SELECT
  `ecw_refnbtlargepractice`.hcaps_group_practice,
  `ecw_refnbtlargepractice`.coid,
  `ecw_refnbtlargepractice`.coidname,
  `ecw_refnbtlargepractice`.coidno,
  `ecw_refnbtlargepractice`.insertedby,
  `ecw_refnbtlargepractice`.inserteddtm,
  `ecw_refnbtlargepractice`.modifiedby,
  `ecw_refnbtlargepractice`.modifieddtm,
  `ecw_refnbtlargepractice`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refnbtlargepractice`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refnbtlargepractice`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;