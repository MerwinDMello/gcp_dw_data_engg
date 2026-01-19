CREATE OR REPLACE VIEW edwpsc_views.`epic_factworkqueueclaimedit`
AS SELECT
  `epic_factworkqueueclaimedit`.regionkey,
  `epic_factworkqueueclaimedit`.regionname,
  `epic_factworkqueueclaimedit`.coid,
  `epic_factworkqueueclaimedit`.workqueuename,
  `epic_factworkqueueclaimedit`.claimkey,
  `epic_factworkqueueclaimedit`.poscode,
  `epic_factworkqueueclaimedit`.visitnumber,
  `epic_factworkqueueclaimedit`.transactionnumber,
  `epic_factworkqueueclaimedit`.invoicenumber,
  `epic_factworkqueueclaimedit`.cptcode,
  `epic_factworkqueueclaimedit`.servicedate,
  `epic_factworkqueueclaimedit`.datecreated,
  `epic_factworkqueueclaimedit`.payorname,
  `epic_factworkqueueclaimedit`.amountdue,
  `epic_factworkqueueclaimedit`.billingprovider,
  `epic_factworkqueueclaimedit`.patientname,
  `epic_factworkqueueclaimedit`.patientmrn,
  `epic_factworkqueueclaimedit`.revlocation,
  `epic_factworkqueueclaimedit`.holdcodekey,
  `epic_factworkqueueclaimedit`.holdcode,
  `epic_factworkqueueclaimedit`.holdcodename,
  `epic_factworkqueueclaimedit`.holdcodedescription,
  `epic_factworkqueueclaimedit`.errormessage,
  `epic_factworkqueueclaimedit`.sourceaprimarykeyvalue,
  `epic_factworkqueueclaimedit`.sourcesystemcode,
  `epic_factworkqueueclaimedit`.insertedby,
  `epic_factworkqueueclaimedit`.inserteddtm,
  `epic_factworkqueueclaimedit`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factworkqueueclaimedit`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factworkqueueclaimedit`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;