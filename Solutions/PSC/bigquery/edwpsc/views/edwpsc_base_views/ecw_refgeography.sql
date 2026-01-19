CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refgeography`
AS SELECT
  `ecw_refgeography`.geographykey,
  `ecw_refgeography`.geographycity,
  `ecw_refgeography`.statekey,
  `ecw_refgeography`.geographyzipcode,
  `ecw_refgeography`.dwlastupdatedatetime,
  `ecw_refgeography`.sourcesystemcode,
  `ecw_refgeography`.insertedby,
  `ecw_refgeography`.inserteddtm,
  `ecw_refgeography`.modifiedby,
  `ecw_refgeography`.modifieddtm
  FROM
    edwpsc.`ecw_refgeography`
;