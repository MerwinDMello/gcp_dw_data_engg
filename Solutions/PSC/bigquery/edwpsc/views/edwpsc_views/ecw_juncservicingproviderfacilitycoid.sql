CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncservicingproviderfacilitycoid`
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
    edwpsc_base_views.`ecw_juncservicingproviderfacilitycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncservicingproviderfacilitycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;