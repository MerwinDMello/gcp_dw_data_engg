CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtewocinventorypk`
AS SELECT
  `ccu_rprtewocinventorypk`.snapshotdate,
  `ccu_rprtewocinventorypk`.sourcesystem,
  `ccu_rprtewocinventorypk`.coid,
  `ccu_rprtewocinventorypk`.owner,
  `ccu_rprtewocinventorypk`.inventorytype,
  `ccu_rprtewocinventorypk`.encounterlock,
  `ccu_rprtewocinventorypk`.poskey,
  `ccu_rprtewocinventorypk`.encounterkey,
  `ccu_rprtewocinventorypk`.businessdayssinceservicedate,
  `ccu_rprtewocinventorypk`.sourcesystemcode,
  `ccu_rprtewocinventorypk`.insertedby,
  `ccu_rprtewocinventorypk`.inserteddtm,
  `ccu_rprtewocinventorypk`.modifiedby,
  `ccu_rprtewocinventorypk`.modifieddtm,
  `ccu_rprtewocinventorypk`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtewocinventorypk`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtewocinventorypk`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;