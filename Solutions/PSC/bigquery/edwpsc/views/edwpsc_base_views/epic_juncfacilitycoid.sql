CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncfacilitycoid`
AS SELECT
  `epic_juncfacilitycoid`.juncfacilitycoidkey,
  `epic_juncfacilitycoid`.facilitykey,
  `epic_juncfacilitycoid`.coid,
  `epic_juncfacilitycoid`.insertedby,
  `epic_juncfacilitycoid`.inserteddtm,
  `epic_juncfacilitycoid`.modifiedby,
  `epic_juncfacilitycoid`.modifieddtm,
  `epic_juncfacilitycoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncfacilitycoid`
;