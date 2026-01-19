CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimlinefinancialadjustment`
AS SELECT
  `ecw_factclaimlinefinancialadjustment`.claimlinefinancialadjustmentskey,
  `ecw_factclaimlinefinancialadjustment`.claimkey,
  `ecw_factclaimlinefinancialadjustment`.claimnumber,
  `ecw_factclaimlinefinancialadjustment`.regionkey,
  `ecw_factclaimlinefinancialadjustment`.coid,
  `ecw_factclaimlinefinancialadjustment`.coidconfigurationkey,
  `ecw_factclaimlinefinancialadjustment`.servicingproviderkey,
  `ecw_factclaimlinefinancialadjustment`.claimpayer1iplankey,
  `ecw_factclaimlinefinancialadjustment`.facilitykey,
  `ecw_factclaimlinefinancialadjustment`.claimadjustmentkey,
  `ecw_factclaimlinefinancialadjustment`.claimlinechargeskey,
  `ecw_factclaimlinefinancialadjustment`.adjustmentamt,
  `ecw_factclaimlinefinancialadjustment`.deleteflag,
  `ecw_factclaimlinefinancialadjustment`.createddatekey,
  `ecw_factclaimlinefinancialadjustment`.createdtime,
  `ecw_factclaimlinefinancialadjustment`.createdbyuserkey,
  `ecw_factclaimlinefinancialadjustment`.modifieddatekey,
  `ecw_factclaimlinefinancialadjustment`.modifiedtime,
  `ecw_factclaimlinefinancialadjustment`.modifiedbyuserkey,
  `ecw_factclaimlinefinancialadjustment`.sourceprimarykeyvalue,
  `ecw_factclaimlinefinancialadjustment`.sourcerecordlastupdated,
  `ecw_factclaimlinefinancialadjustment`.dwlastupdatedatetime,
  `ecw_factclaimlinefinancialadjustment`.sourcesystemcode,
  `ecw_factclaimlinefinancialadjustment`.insertedby,
  `ecw_factclaimlinefinancialadjustment`.inserteddtm,
  `ecw_factclaimlinefinancialadjustment`.modifiedby,
  `ecw_factclaimlinefinancialadjustment`.modifieddtm,
  `ecw_factclaimlinefinancialadjustment`.cptid,
  `ecw_factclaimlinefinancialadjustment`.cptcode,
  `ecw_factclaimlinefinancialadjustment`.cptcodekey,
  `ecw_factclaimlinefinancialadjustment`.cptcharges,
  `ecw_factclaimlinefinancialadjustment`.cptdeleteflag,
  `ecw_factclaimlinefinancialadjustment`.adjustmentcodekey,
  `ecw_factclaimlinefinancialadjustment`.claimadjustmentamt,
  `ecw_factclaimlinefinancialadjustment`.claimadjustmentdatekey,
  `ecw_factclaimlinefinancialadjustment`.claimadjustmentdeleteflag,
  `ecw_factclaimlinefinancialadjustment`.adjcode,
  `ecw_factclaimlinefinancialadjustment`.adjustmentcategorykey,
  `ecw_factclaimlinefinancialadjustment`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimlinefinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimlinefinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;