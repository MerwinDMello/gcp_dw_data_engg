CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencounter`
AS SELECT
  `ecw_factencounter`.encounterkey,
  `ecw_factencounter`.claimkey,
  `ecw_factencounter`.regionkey,
  `ecw_factencounter`.coid,
  `ecw_factencounter`.coidconfigurationkey,
  `ecw_factencounter`.servicingproviderkey,
  `ecw_factencounter`.facilitykey,
  `ecw_factencounter`.patientkey,
  `ecw_factencounter`.visitreason,
  `ecw_factencounter`.visitdate,
  `ecw_factencounter`.visitstarttime,
  `ecw_factencounter`.visitendtime,
  `ecw_factencounter`.visittypekey,
  `ecw_factencounter`.visitcopay,
  `ecw_factencounter`.visitstatuskey,
  `ecw_factencounter`.encountertype,
  `ecw_factencounter`.deleteflag,
  `ecw_factencounter`.encounterlock,
  `ecw_factencounter`.visitarrivedtime,
  `ecw_factencounter`.statusaftercheckin,
  `ecw_factencounter`.claimrequired,
  `ecw_factencounter`.visittypeoverriden,
  `ecw_factencounter`.claimnumber,
  `ecw_factencounter`.referringproviderkey,
  `ecw_factencounter`.facilityresourcekey,
  `ecw_factencounter`.timein,
  `ecw_factencounter`.timeout,
  `ecw_factencounter`.departuretime,
  `ecw_factencounter`.sourceprimarykeyvalue,
  `ecw_factencounter`.sourcerecordlastupdated,
  `ecw_factencounter`.dwlastupdatedatetime,
  `ecw_factencounter`.sourcesystemcode,
  `ecw_factencounter`.insertedby,
  `ecw_factencounter`.inserteddtm,
  `ecw_factencounter`.modifiedby,
  `ecw_factencounter`.modifieddtm,
  `ecw_factencounter`.sourcepatientacctnumber,
  `ecw_factencounter`.appointmentcreateddate,
  `ecw_factencounter`.appointmentcreatedby,
  `ecw_factencounter`.gfestatus,
  `ecw_factencounter`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factencounter`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factencounter`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;