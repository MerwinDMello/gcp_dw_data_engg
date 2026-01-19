CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimdiagnosis`
AS SELECT
  `epic_factclaimdiagnosis`.claimdiagnosiskey,
  `epic_factclaimdiagnosis`.claimkey,
  `epic_factclaimdiagnosis`.claimnumber,
  `epic_factclaimdiagnosis`.visitnumber,
  `epic_factclaimdiagnosis`.regionkey,
  `epic_factclaimdiagnosis`.coid,
  `epic_factclaimdiagnosis`.coidconfigurationkey,
  `epic_factclaimdiagnosis`.servicingproviderkey,
  `epic_factclaimdiagnosis`.claimpayer1iplankey,
  `epic_factclaimdiagnosis`.facilitykey,
  `epic_factclaimdiagnosis`.diagnosiscodekey,
  `epic_factclaimdiagnosis`.primarycode,
  `epic_factclaimdiagnosis`.icdorder,
  `epic_factclaimdiagnosis`.deleteflag,
  `epic_factclaimdiagnosis`.sourceaprimarykeyvalue,
  `epic_factclaimdiagnosis`.sourcearecordlastupdated,
  `epic_factclaimdiagnosis`.sourcebprimarykeyvalue,
  `epic_factclaimdiagnosis`.sourcebrecordlastupdated,
  `epic_factclaimdiagnosis`.dwlastupdatedatetime,
  `epic_factclaimdiagnosis`.sourcesystemcode,
  `epic_factclaimdiagnosis`.insertedby,
  `epic_factclaimdiagnosis`.inserteddtm,
  `epic_factclaimdiagnosis`.modifiedby,
  `epic_factclaimdiagnosis`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimdiagnosis`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimdiagnosis`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;