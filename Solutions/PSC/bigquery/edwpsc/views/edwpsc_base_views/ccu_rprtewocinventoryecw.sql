CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtewocinventoryecw`
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
    edwpsc.`ccu_rprtewocinventoryecw`
;