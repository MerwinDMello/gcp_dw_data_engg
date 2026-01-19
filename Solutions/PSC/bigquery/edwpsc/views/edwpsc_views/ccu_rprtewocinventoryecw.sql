CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtewocinventoryecw`
AS SELECT
  `ccu_rprtewocinventoryecw`.snapshotdate,
  `ccu_rprtewocinventoryecw`.coid,
  `ccu_rprtewocinventoryecw`.poskey,
  `ccu_rprtewocinventoryecw`.encounterkey,
  `ccu_rprtewocinventoryecw`.encounterlock,
  `ccu_rprtewocinventoryecw`.sourcesystemcode,
  `ccu_rprtewocinventoryecw`.insertedby,
  `ccu_rprtewocinventoryecw`.inserteddtm,
  `ccu_rprtewocinventoryecw`.modifiedby,
  `ccu_rprtewocinventoryecw`.modifieddtm,
  `ccu_rprtewocinventoryecw`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtewocinventoryecw`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtewocinventoryecw`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;