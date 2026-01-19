CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refgeography`
AS SELECT
  `epic_refgeography`.geographykey,
  `epic_refgeography`.geographycity,
  `epic_refgeography`.statekey,
  `epic_refgeography`.geographyzipcode,
  `epic_refgeography`.dwlastupdatedatetime,
  `epic_refgeography`.sourcesystemcode,
  `epic_refgeography`.insertedby,
  `epic_refgeography`.inserteddtm,
  `epic_refgeography`.modifiedby,
  `epic_refgeography`.modifieddtm
  FROM
    edwpsc.`epic_refgeography`
;