CREATE OR REPLACE VIEW edwpsc_views.`epic_juncfacilitycoid`
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
    edwpsc_base_views.`epic_juncfacilitycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_juncfacilitycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;