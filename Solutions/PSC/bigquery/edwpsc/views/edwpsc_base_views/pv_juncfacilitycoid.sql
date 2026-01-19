CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncfacilitycoid`
AS SELECT
  `pv_juncfacilitycoid`.juncfacilitycoidkey,
  `pv_juncfacilitycoid`.facilitykey,
  `pv_juncfacilitycoid`.coid,
  `pv_juncfacilitycoid`.insertedby,
  `pv_juncfacilitycoid`.inserteddtm,
  `pv_juncfacilitycoid`.modifiedby,
  `pv_juncfacilitycoid`.modifieddtm,
  `pv_juncfacilitycoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_juncfacilitycoid`
;