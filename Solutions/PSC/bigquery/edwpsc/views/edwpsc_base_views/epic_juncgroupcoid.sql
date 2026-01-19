CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncgroupcoid`
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
    edwpsc.`epic_juncgroupcoid`
;