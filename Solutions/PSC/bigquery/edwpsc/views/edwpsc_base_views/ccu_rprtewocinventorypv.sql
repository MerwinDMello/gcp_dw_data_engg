CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtewocinventorypv`
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
    edwpsc.`ccu_rprtewocinventorypv`
;