CREATE OR REPLACE VIEW edwpsc_views.`epic_juncgroupcoid`
AS SELECT
  `epic_juncgroupcoid`.juncgroupcoidkey,
  `epic_juncgroupcoid`.groupkey,
  `epic_juncgroupcoid`.coid,
  `epic_juncgroupcoid`.insertedby,
  `epic_juncgroupcoid`.inserteddtm,
  `epic_juncgroupcoid`.modifiedby,
  `epic_juncgroupcoid`.modifieddtm,
  `epic_juncgroupcoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_juncgroupcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncgroupcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;