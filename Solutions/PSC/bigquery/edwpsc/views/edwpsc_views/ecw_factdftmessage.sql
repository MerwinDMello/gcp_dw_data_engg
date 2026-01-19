CREATE OR REPLACE VIEW edwpsc_views.`ecw_factdftmessage`
AS SELECT
  `ecw_factdftmessage`.dftmessagekey,
  `ecw_factdftmessage`.regionkey,
  `ecw_factdftmessage`.datereceived,
  `ecw_factdftmessage`.datemodified,
  `ecw_factdftmessage`.patientaccountnumber,
  `ecw_factdftmessage`.patientlastname,
  `ecw_factdftmessage`.patientfirstname,
  `ecw_factdftmessage`.patientdobkey,
  `ecw_factdftmessage`.messagestatus,
  `ecw_factdftmessage`.visitnumber,
  `ecw_factdftmessage`.encounterid,
  `ecw_factdftmessage`.encounterkey,
  `ecw_factdftmessage`.claimid,
  `ecw_factdftmessage`.claimkey,
  `ecw_factdftmessage`.errormessage,
  `ecw_factdftmessage`.messagecontrolid,
  `ecw_factdftmessage`.providerid,
  `ecw_factdftmessage`.providerkey,
  `ecw_factdftmessage`.facilityid,
  `ecw_factdftmessage`.facilitykey,
  `ecw_factdftmessage`.sentid,
  `ecw_factdftmessage`.messagetype,
  `ecw_factdftmessage`.reconciledflag,
  `ecw_factdftmessage`.practiceid,
  `ecw_factdftmessage`.pmid,
  `ecw_factdftmessage`.sourceaprimarykeyvalue,
  `ecw_factdftmessage`.dwlastupdatedatetime,
  `ecw_factdftmessage`.sourcesystemcode,
  `ecw_factdftmessage`.insertedby,
  `ecw_factdftmessage`.inserteddtm,
  `ecw_factdftmessage`.modifiedby,
  `ecw_factdftmessage`.modifieddtm,
  `ecw_factdftmessage`.messagecontrolidshort,
  `ecw_factdftmessage`.pmidname,
  `ecw_factdftmessage`.chargetransactionid,
  `ecw_factdftmessage`.sendingfacility,
  `ecw_factdftmessage`.visitlocationfacility,
  `ecw_factdftmessage`.visitlocationunit,
  `ecw_factdftmessage`.visitlocation,
  `ecw_factdftmessage`.coid
  FROM
    edwpsc_base_views.`ecw_factdftmessage`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factdftmessage`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;