CREATE OR REPLACE VIEW edwpsc_views.`pv_factclaimlinefinancialadjustment`
AS SELECT
  `pv_factclaimlinefinancialadjustment`.claimlinefinancialadjustmentskey,
  `pv_factclaimlinefinancialadjustment`.claimkey,
  `pv_factclaimlinefinancialadjustment`.claimnumber,
  `pv_factclaimlinefinancialadjustment`.regionkey,
  `pv_factclaimlinefinancialadjustment`.coid,
  `pv_factclaimlinefinancialadjustment`.coidconfigurationkey,
  `pv_factclaimlinefinancialadjustment`.servicingproviderkey,
  `pv_factclaimlinefinancialadjustment`.claimpayer1iplankey,
  `pv_factclaimlinefinancialadjustment`.facilitykey,
  `pv_factclaimlinefinancialadjustment`.claimadjustmentkey,
  `pv_factclaimlinefinancialadjustment`.claimlinechargeskey,
  `pv_factclaimlinefinancialadjustment`.adjustmentamt,
  `pv_factclaimlinefinancialadjustment`.adjustmentcodekey,
  `pv_factclaimlinefinancialadjustment`.adjcode,
  `pv_factclaimlinefinancialadjustment`.adjustmentcategorykey,
  `pv_factclaimlinefinancialadjustment`.cptid,
  `pv_factclaimlinefinancialadjustment`.cptcode,
  `pv_factclaimlinefinancialadjustment`.cptcodekey,
  `pv_factclaimlinefinancialadjustment`.cptcharges,
  `pv_factclaimlinefinancialadjustment`.cptdeleteflag,
  `pv_factclaimlinefinancialadjustment`.claimadjustmentamt,
  `pv_factclaimlinefinancialadjustment`.claimadjustmentdatekey,
  `pv_factclaimlinefinancialadjustment`.claimadjustmentdeleteflag,
  `pv_factclaimlinefinancialadjustment`.deleteflag,
  `pv_factclaimlinefinancialadjustment`.createddatekey,
  `pv_factclaimlinefinancialadjustment`.createdtime,
  `pv_factclaimlinefinancialadjustment`.createdbyuserkey,
  `pv_factclaimlinefinancialadjustment`.closingdatekey,
  `pv_factclaimlinefinancialadjustment`.sourceaprimarykeyvalue,
  `pv_factclaimlinefinancialadjustment`.sourcearecordlastupdated,
  `pv_factclaimlinefinancialadjustment`.sourcebprimarykeyvalue,
  `pv_factclaimlinefinancialadjustment`.dwlastupdatedatetime,
  `pv_factclaimlinefinancialadjustment`.sourcesystemcode,
  `pv_factclaimlinefinancialadjustment`.insertedby,
  `pv_factclaimlinefinancialadjustment`.inserteddtm,
  `pv_factclaimlinefinancialadjustment`.modifiedby,
  `pv_factclaimlinefinancialadjustment`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factclaimlinefinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factclaimlinefinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;