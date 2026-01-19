CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncdivisioncoid`
AS SELECT
  `epic_juncdivisioncoid`.juncdivisioncoidkey,
  `epic_juncdivisioncoid`.divisionkey,
  `epic_juncdivisioncoid`.coid,
  `epic_juncdivisioncoid`.insertedby,
  `epic_juncdivisioncoid`.inserteddtm,
  `epic_juncdivisioncoid`.modifiedby,
  `epic_juncdivisioncoid`.modifieddtm,
  `epic_juncdivisioncoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncdivisioncoid`
;