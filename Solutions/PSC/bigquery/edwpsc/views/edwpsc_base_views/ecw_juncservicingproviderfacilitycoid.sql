CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncservicingproviderfacilitycoid`
AS SELECT
  `ecw_juncservicingproviderfacilitycoid`.juncservicingproviderfacilitycoidkey,
  `ecw_juncservicingproviderfacilitycoid`.providerkey,
  `ecw_juncservicingproviderfacilitycoid`.facilitykey,
  `ecw_juncservicingproviderfacilitycoid`.coid,
  `ecw_juncservicingproviderfacilitycoid`.insertedby,
  `ecw_juncservicingproviderfacilitycoid`.inserteddtm,
  `ecw_juncservicingproviderfacilitycoid`.modifiedby,
  `ecw_juncservicingproviderfacilitycoid`.modifieddtm,
  `ecw_juncservicingproviderfacilitycoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncservicingproviderfacilitycoid`
;