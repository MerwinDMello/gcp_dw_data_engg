CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncfacilitycoid`
AS SELECT
  `ecw_juncfacilitycoid`.juncfacilitycoidkey,
  `ecw_juncfacilitycoid`.facilitykey,
  `ecw_juncfacilitycoid`.coid,
  `ecw_juncfacilitycoid`.insertedby,
  `ecw_juncfacilitycoid`.inserteddtm,
  `ecw_juncfacilitycoid`.modifiedby,
  `ecw_juncfacilitycoid`.modifieddtm,
  `ecw_juncfacilitycoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncfacilitycoid`
;