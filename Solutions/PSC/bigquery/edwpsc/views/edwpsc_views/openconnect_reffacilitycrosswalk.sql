CREATE OR REPLACE VIEW edwpsc_views.`openconnect_reffacilitycrosswalk`
AS SELECT
  `openconnect_reffacilitycrosswalk`.openconnectfacilitycrosswalkkey,
  `openconnect_reffacilitycrosswalk`.regionkey,
  `openconnect_reffacilitycrosswalk`.sendingapplication,
  `openconnect_reffacilitycrosswalk`.facilityid,
  `openconnect_reffacilitycrosswalk`.practice,
  `openconnect_reffacilitycrosswalk`.visitlocationunit,
  `openconnect_reffacilitycrosswalk`.patientclass,
  `openconnect_reffacilitycrosswalk`.ecwhl7id,
  `openconnect_reffacilitycrosswalk`.beginactive,
  `openconnect_reffacilitycrosswalk`.endactive,
  `openconnect_reffacilitycrosswalk`.crosswalkcreatedby,
  `openconnect_reffacilitycrosswalk`.crosswalkcreateddate,
  `openconnect_reffacilitycrosswalk`.crosswalkmodifiedby,
  `openconnect_reffacilitycrosswalk`.crosswalkmodifieddate,
  `openconnect_reffacilitycrosswalk`.deletedflag,
  `openconnect_reffacilitycrosswalk`.ruralhealthflag,
  `openconnect_reffacilitycrosswalk`.version,
  `openconnect_reffacilitycrosswalk`.sourceaprimarykeyvalue,
  `openconnect_reffacilitycrosswalk`.dwlastupdatedatetime,
  `openconnect_reffacilitycrosswalk`.sourcesystemcode,
  `openconnect_reffacilitycrosswalk`.insertedby,
  `openconnect_reffacilitycrosswalk`.inserteddtm,
  `openconnect_reffacilitycrosswalk`.modifiedby,
  `openconnect_reffacilitycrosswalk`.modifieddtm,
  `openconnect_reffacilitycrosswalk`.coid
  FROM
    edwpsc_base_views.`openconnect_reffacilitycrosswalk`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`openconnect_reffacilitycrosswalk`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;