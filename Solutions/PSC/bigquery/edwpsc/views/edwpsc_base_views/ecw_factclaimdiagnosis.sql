CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factclaimdiagnosis`
AS SELECT
  `ecw_factclaimdiagnosis`.claimdiagnosiskey,
  `ecw_factclaimdiagnosis`.claimkey,
  `ecw_factclaimdiagnosis`.claimnumber,
  `ecw_factclaimdiagnosis`.regionkey,
  `ecw_factclaimdiagnosis`.coid,
  `ecw_factclaimdiagnosis`.coidconfigurationkey,
  `ecw_factclaimdiagnosis`.servicingproviderkey,
  `ecw_factclaimdiagnosis`.claimpayer1iplankey,
  `ecw_factclaimdiagnosis`.facilitykey,
  `ecw_factclaimdiagnosis`.diagnosiscodekey,
  `ecw_factclaimdiagnosis`.primarycode,
  `ecw_factclaimdiagnosis`.icdorder,
  `ecw_factclaimdiagnosis`.sourceprimarykeyvalue,
  `ecw_factclaimdiagnosis`.sourcerecordlastupdated,
  `ecw_factclaimdiagnosis`.dwlastupdatedatetime,
  `ecw_factclaimdiagnosis`.sourcesystemcode,
  `ecw_factclaimdiagnosis`.insertedby,
  `ecw_factclaimdiagnosis`.inserteddtm,
  `ecw_factclaimdiagnosis`.modifiedby,
  `ecw_factclaimdiagnosis`.modifieddtm,
  `ecw_factclaimdiagnosis`.deleteflag,
  `ecw_factclaimdiagnosis`.archivedrecord
  FROM
    edwpsc.`ecw_factclaimdiagnosis`
;