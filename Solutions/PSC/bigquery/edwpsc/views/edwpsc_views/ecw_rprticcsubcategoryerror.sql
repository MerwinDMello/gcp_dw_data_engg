CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprticcsubcategoryerror`
AS SELECT
  `ecw_rprticcsubcategoryerror`.snapshotdate,
  `ecw_rprticcsubcategoryerror`.claimkey,
  `ecw_rprticcsubcategoryerror`.coid,
  `ecw_rprticcsubcategoryerror`.hcpatientaccountingnumber,
  `ecw_rprticcsubcategoryerror`.hcenid,
  `ecw_rprticcsubcategoryerror`.pssccat,
  `ecw_rprticcsubcategoryerror`.subcategorydescription,
  `ecw_rprticcsubcategoryerror`.psscid,
  `ecw_rprticcsubcategoryerror`.createdate,
  `ecw_rprticcsubcategoryerror`.completeflag,
  `ecw_rprticcsubcategoryerror`.closeddate,
  `ecw_rprticcsubcategoryerror`.currentlyopen,
  `ecw_rprticcsubcategoryerror`.createdinsnapshot,
  `ecw_rprticcsubcategoryerror`.closedinsnapshot,
  `ecw_rprticcsubcategoryerror`.age,
  `ecw_rprticcsubcategoryerror`.originalroute,
  `ecw_rprticcsubcategoryerror`.totalbalanceamt,
  `ecw_rprticcsubcategoryerror`.servicingproviderkey,
  `ecw_rprticcsubcategoryerror`.insertedby,
  `ecw_rprticcsubcategoryerror`.inserteddtm,
  `ecw_rprticcsubcategoryerror`.modifiedby,
  `ecw_rprticcsubcategoryerror`.modifieddtm,
  `ecw_rprticcsubcategoryerror`.psfaietreroute,
  `ecw_rprticcsubcategoryerror`.psfaietactdte,
  `ecw_rprticcsubcategoryerror`.psencoidgrp,
  `ecw_rprticcsubcategoryerror`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprticcsubcategoryerror`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprticcsubcategoryerror`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;