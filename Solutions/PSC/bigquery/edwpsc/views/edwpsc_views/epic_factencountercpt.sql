CREATE OR REPLACE VIEW edwpsc_views.`epic_factencountercpt`
AS SELECT
  `epic_factencountercpt`.encountercptkey,
  `epic_factencountercpt`.regionkey,
  `epic_factencountercpt`.coid,
  `epic_factencountercpt`.claimkey,
  `epic_factencountercpt`.encounterkey,
  `epic_factencountercpt`.encounterid,
  `epic_factencountercpt`.patientinternalid,
  `epic_factencountercpt`.cptcodekey,
  `epic_factencountercpt`.cptcode,
  `epic_factencountercpt`.cptunits,
  `epic_factencountercpt`.cptcharges,
  `epic_factencountercpt`.visitdate,
  `epic_factencountercpt`.primaryflag,
  `epic_factencountercpt`.deleteflag,
  `epic_factencountercpt`.createdbyuserkey,
  `epic_factencountercpt`.deletedbyuserkey,
  `epic_factencountercpt`.deletedondatekey,
  `epic_factencountercpt`.encounterstatus,
  `epic_factencountercpt`.accountid,
  `epic_factencountercpt`.servicingproviderkey,
  `epic_factencountercpt`.servicingproviderid,
  `epic_factencountercpt`.billingproviderkey,
  `epic_factencountercpt`.billingproviderid,
  `epic_factencountercpt`.patientkey,
  `epic_factencountercpt`.patientid,
  `epic_factencountercpt`.practicekey,
  `epic_factencountercpt`.practiceid,
  `epic_factencountercpt`.transactionid,
  `epic_factencountercpt`.sourceaprimarykeyvalue,
  `epic_factencountercpt`.sourcebprimarykeyvalue,
  `epic_factencountercpt`.sourcecprimarykeyvalue,
  `epic_factencountercpt`.sourcerecordlastupdated,
  `epic_factencountercpt`.dwlastupdatedatetime,
  `epic_factencountercpt`.sourcesystemcode,
  `epic_factencountercpt`.insertedby,
  `epic_factencountercpt`.inserteddtm,
  `epic_factencountercpt`.modifiedby,
  `epic_factencountercpt`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factencountercpt`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factencountercpt`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;