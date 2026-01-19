CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtewocinventorypv`
AS SELECT
  `ccu_rprtewocinventorypv`.snapshotdate,
  `ccu_rprtewocinventorypv`.coid,
  `ccu_rprtewocinventorypv`.practicename,
  `ccu_rprtewocinventorypv`.encounterkey,
  `ccu_rprtewocinventorypv`.encounterlock,
  `ccu_rprtewocinventorypv`.sourcesystemcode,
  `ccu_rprtewocinventorypv`.insertedby,
  `ccu_rprtewocinventorypv`.inserteddtm,
  `ccu_rprtewocinventorypv`.modifiedby,
  `ccu_rprtewocinventorypv`.modifieddtm,
  `ccu_rprtewocinventorypv`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtewocinventorypv`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtewocinventorypv`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;