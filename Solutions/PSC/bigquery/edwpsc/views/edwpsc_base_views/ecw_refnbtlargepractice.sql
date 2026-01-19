CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtlargepractice`
AS SELECT
  `ecw_refnbtlargepractice`.hcaps_group_practice,
  `ecw_refnbtlargepractice`.coid,
  `ecw_refnbtlargepractice`.coidname,
  `ecw_refnbtlargepractice`.coidno,
  `ecw_refnbtlargepractice`.insertedby,
  `ecw_refnbtlargepractice`.inserteddtm,
  `ecw_refnbtlargepractice`.modifiedby,
  `ecw_refnbtlargepractice`.modifieddtm,
  `ecw_refnbtlargepractice`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refnbtlargepractice`
;