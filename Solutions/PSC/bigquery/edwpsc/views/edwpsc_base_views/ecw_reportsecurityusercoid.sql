CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reportsecurityusercoid`
AS SELECT
  `ecw_reportsecurityusercoid`.fulluser34,
  `ecw_reportsecurityusercoid`.userdomain,
  `ecw_reportsecurityusercoid`.user34,
  `ecw_reportsecurityusercoid`.userpbi,
  `ecw_reportsecurityusercoid`.coid,
  `ecw_reportsecurityusercoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_reportsecurityusercoid`
;