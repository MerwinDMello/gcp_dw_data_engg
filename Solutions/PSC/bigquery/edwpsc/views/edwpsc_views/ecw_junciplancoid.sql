CREATE OR REPLACE VIEW edwpsc_views.`ecw_junciplancoid`
AS SELECT
  `ecw_junciplancoid`.junciplancoidkey,
  `ecw_junciplancoid`.iplankey,
  `ecw_junciplancoid`.coid,
  `ecw_junciplancoid`.insertedby,
  `ecw_junciplancoid`.inserteddtm,
  `ecw_junciplancoid`.modifiedby,
  `ecw_junciplancoid`.modifieddtm,
  `ecw_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_junciplancoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_junciplancoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;