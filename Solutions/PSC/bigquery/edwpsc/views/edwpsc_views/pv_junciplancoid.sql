CREATE OR REPLACE VIEW edwpsc_views.`pv_junciplancoid`
AS SELECT
  `pv_junciplancoid`.junciplancoidkey,
  `pv_junciplancoid`.iplankey,
  `pv_junciplancoid`.coid,
  `pv_junciplancoid`.insertedby,
  `pv_junciplancoid`.inserteddtm,
  `pv_junciplancoid`.modifiedby,
  `pv_junciplancoid`.modifieddtm,
  `pv_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_junciplancoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_junciplancoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;