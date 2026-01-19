CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncgroupcoid`
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
    edwpsc.`ecw_juncgroupcoid`
;