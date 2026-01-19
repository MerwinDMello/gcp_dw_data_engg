CREATE OR REPLACE VIEW edwpsc_views.`epic_juncmarketcoid`
AS SELECT
  `epic_juncmarketcoid`.juncmarketcoidkey,
  `epic_juncmarketcoid`.marketkey,
  `epic_juncmarketcoid`.coid,
  `epic_juncmarketcoid`.insertedby,
  `epic_juncmarketcoid`.inserteddtm,
  `epic_juncmarketcoid`.modifiedby,
  `epic_juncmarketcoid`.modifieddtm,
  `epic_juncmarketcoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_juncmarketcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncmarketcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;