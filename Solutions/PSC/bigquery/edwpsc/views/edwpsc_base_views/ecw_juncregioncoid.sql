CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncregioncoid`
AS SELECT
  `ecw_juncregioncoid`.juncregioncoidkey,
  `ecw_juncregioncoid`.regionkey,
  `ecw_juncregioncoid`.coid,
  `ecw_juncregioncoid`.insertedby,
  `ecw_juncregioncoid`.inserteddtm,
  `ecw_juncregioncoid`.modifiedby,
  `ecw_juncregioncoid`.modifieddtm,
  `ecw_juncregioncoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncregioncoid`
;