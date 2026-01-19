CREATE OR REPLACE VIEW edwpsc_views.`epic_junciplancoid`
AS SELECT
  `epic_junciplancoid`.junciplancoidkey,
  `epic_junciplancoid`.iplankey,
  `epic_junciplancoid`.coid,
  `epic_junciplancoid`.insertedby,
  `epic_junciplancoid`.inserteddtm,
  `epic_junciplancoid`.modifiedby,
  `epic_junciplancoid`.modifieddtm,
  `epic_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_junciplancoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_junciplancoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;