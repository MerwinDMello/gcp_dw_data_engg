CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncmarketcoid`
AS SELECT
  `ecw_juncmarketcoid`.juncmarketcoidkey,
  `ecw_juncmarketcoid`.marketkey,
  `ecw_juncmarketcoid`.coid,
  `ecw_juncmarketcoid`.insertedby,
  `ecw_juncmarketcoid`.inserteddtm,
  `ecw_juncmarketcoid`.modifiedby,
  `ecw_juncmarketcoid`.modifieddtm,
  `ecw_juncmarketcoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncmarketcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncmarketcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;