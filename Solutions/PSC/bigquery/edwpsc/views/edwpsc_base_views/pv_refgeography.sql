CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refgeography`
AS SELECT
  `pv_refgeography`.geographykey,
  `pv_refgeography`.geographycity,
  `pv_refgeography`.statekey,
  `pv_refgeography`.geographyzipcode,
  `pv_refgeography`.dwlastupdatedatetime,
  `pv_refgeography`.sourcesystemcode,
  `pv_refgeography`.insertedby,
  `pv_refgeography`.inserteddtm,
  `pv_refgeography`.modifiedby,
  `pv_refgeography`.modifieddtm
  FROM
    edwpsc.`pv_refgeography`
;