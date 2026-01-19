CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimlinefinancialadjustment`
AS SELECT
  `epic_factclaimlinefinancialadjustment`.claimlinefinancialadjustmentskey,
  `epic_factclaimlinefinancialadjustment`.claimkey,
  `epic_factclaimlinefinancialadjustment`.claimnumber,
  `epic_factclaimlinefinancialadjustment`.transactionnumber,
  `epic_factclaimlinefinancialadjustment`.visitnumber,
  `epic_factclaimlinefinancialadjustment`.regionkey,
  `epic_factclaimlinefinancialadjustment`.coid,
  `epic_factclaimlinefinancialadjustment`.coidconfigurationkey,
  `epic_factclaimlinefinancialadjustment`.servicingproviderkey,
  `epic_factclaimlinefinancialadjustment`.claimpayer1iplankey,
  `epic_factclaimlinefinancialadjustment`.claimlineliabilityiplankey,
  `epic_factclaimlinefinancialadjustment`.facilitykey,
  `epic_factclaimlinefinancialadjustment`.claimadjustmentkey,
  `epic_factclaimlinefinancialadjustment`.claimlinechargeskey,
  `epic_factclaimlinefinancialadjustment`.adjustmentamt,
  `epic_factclaimlinefinancialadjustment`.adjustmentcodekey,
  `epic_factclaimlinefinancialadjustment`.adjcode,
  `epic_factclaimlinefinancialadjustment`.adjustmentcategorykey,
  `epic_factclaimlinefinancialadjustment`.cptid,
  `epic_factclaimlinefinancialadjustment`.cptcode,
  `epic_factclaimlinefinancialadjustment`.cptcodekey,
  `epic_factclaimlinefinancialadjustment`.cptcharges,
  `epic_factclaimlinefinancialadjustment`.claimadjustmentamt,
  `epic_factclaimlinefinancialadjustment`.claimadjustmentdatekey,
  `epic_factclaimlinefinancialadjustment`.claimadjustmentdeleteflag,
  `epic_factclaimlinefinancialadjustment`.deleteflag,
  `epic_factclaimlinefinancialadjustment`.createddatekey,
  `epic_factclaimlinefinancialadjustment`.createdtime,
  `epic_factclaimlinefinancialadjustment`.createdbyuserkey,
  `epic_factclaimlinefinancialadjustment`.cptdeleteflag,
  `epic_factclaimlinefinancialadjustment`.sourceaprimarykeyvalue,
  `epic_factclaimlinefinancialadjustment`.sourcearecordlastupdated,
  `epic_factclaimlinefinancialadjustment`.sourcebprimarykeyvalue,
  `epic_factclaimlinefinancialadjustment`.dwlastupdatedatetime,
  `epic_factclaimlinefinancialadjustment`.sourcesystemcode,
  `epic_factclaimlinefinancialadjustment`.insertedby,
  `epic_factclaimlinefinancialadjustment`.inserteddtm,
  `epic_factclaimlinefinancialadjustment`.modifiedby,
  `epic_factclaimlinefinancialadjustment`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimlinefinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimlinefinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;