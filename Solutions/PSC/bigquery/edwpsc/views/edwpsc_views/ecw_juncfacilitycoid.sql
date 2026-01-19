CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncfacilitycoid`
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
    edwpsc_base_views.`ecw_juncfacilitycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncfacilitycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;