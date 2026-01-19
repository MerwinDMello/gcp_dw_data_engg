CREATE OR REPLACE VIEW edwpsc_views.`pv_juncfacilitycoid`
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
    edwpsc_base_views.`pv_juncfacilitycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_juncfacilitycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;