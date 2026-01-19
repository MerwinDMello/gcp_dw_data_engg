CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncdivisioncoid`
AS SELECT
  `ecw_juncdivisioncoid`.juncdivisioncoidkey,
  `ecw_juncdivisioncoid`.divisionkey,
  `ecw_juncdivisioncoid`.coid,
  `ecw_juncdivisioncoid`.insertedby,
  `ecw_juncdivisioncoid`.inserteddtm,
  `ecw_juncdivisioncoid`.modifiedby,
  `ecw_juncdivisioncoid`.modifieddtm,
  `ecw_juncdivisioncoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncdivisioncoid`
;