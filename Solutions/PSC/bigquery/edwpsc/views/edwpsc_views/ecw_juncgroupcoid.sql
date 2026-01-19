CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncgroupcoid`
AS SELECT
  `ecw_juncgroupcoid`.juncgroupcoidkey,
  `ecw_juncgroupcoid`.groupkey,
  `ecw_juncgroupcoid`.coid,
  `ecw_juncgroupcoid`.insertedby,
  `ecw_juncgroupcoid`.inserteddtm,
  `ecw_juncgroupcoid`.modifiedby,
  `ecw_juncgroupcoid`.modifieddtm,
  `ecw_juncgroupcoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncgroupcoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncgroupcoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;