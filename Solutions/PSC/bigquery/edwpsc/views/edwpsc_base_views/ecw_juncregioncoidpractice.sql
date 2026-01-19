CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncregioncoidpractice`
AS SELECT
  `ecw_juncregioncoidpractice`.juncregioncoidpracticekey,
  `ecw_juncregioncoidpractice`.regionkey,
  `ecw_juncregioncoidpractice`.coid,
  `ecw_juncregioncoidpractice`.practicekey,
  `ecw_juncregioncoidpractice`.insertedby,
  `ecw_juncregioncoidpractice`.inserteddtm,
  `ecw_juncregioncoidpractice`.modifiedby,
  `ecw_juncregioncoidpractice`.modifieddtm,
  `ecw_juncregioncoidpractice`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncregioncoidpractice`
;