CREATE OR REPLACE VIEW edwpsc_views.`pv_factclaimfinancialadjustment`
AS SELECT
  `pv_factclaimfinancialadjustment`.claimfinancialadjustmentkey,
  `pv_factclaimfinancialadjustment`.claimkey,
  `pv_factclaimfinancialadjustment`.claimnumber,
  `pv_factclaimfinancialadjustment`.regionkey,
  `pv_factclaimfinancialadjustment`.coid,
  `pv_factclaimfinancialadjustment`.coidconfigurationkey,
  `pv_factclaimfinancialadjustment`.servicingproviderkey,
  `pv_factclaimfinancialadjustment`.claimpayer1iplankey,
  `pv_factclaimfinancialadjustment`.facilitykey,
  `pv_factclaimfinancialadjustment`.adjustmentcodekey,
  `pv_factclaimfinancialadjustment`.adjustmentamt,
  `pv_factclaimfinancialadjustment`.adjustmentdatekey,
  `pv_factclaimfinancialadjustment`.unpostedtocptamt,
  `pv_factclaimfinancialadjustment`.unpostedamt,
  `pv_factclaimfinancialadjustment`.modifieddatekey,
  `pv_factclaimfinancialadjustment`.modifiedtime,
  `pv_factclaimfinancialadjustment`.createdbyuserkey,
  `pv_factclaimfinancialadjustment`.closingdatekey,
  `pv_factclaimfinancialadjustment`.deleteflag,
  `pv_factclaimfinancialadjustment`.sourceaprimarykeyvalue,
  `pv_factclaimfinancialadjustment`.sourcearecordlastupdated,
  `pv_factclaimfinancialadjustment`.sourcebprimarykeyvalue,
  `pv_factclaimfinancialadjustment`.sourcebrecordlastupdated,
  `pv_factclaimfinancialadjustment`.dwlastupdatedatetime,
  `pv_factclaimfinancialadjustment`.sourcesystemcode,
  `pv_factclaimfinancialadjustment`.insertedby,
  `pv_factclaimfinancialadjustment`.inserteddtm,
  `pv_factclaimfinancialadjustment`.modifiedby,
  `pv_factclaimfinancialadjustment`.modifieddtm,
  `pv_factclaimfinancialadjustment`.adjcode,
  `pv_factclaimfinancialadjustment`.adjustmentcategorykey,
  `pv_factclaimfinancialadjustment`.practicekey,
  `pv_factclaimfinancialadjustment`.practicename
  FROM
    edwpsc_base_views.`pv_factclaimfinancialadjustment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factclaimfinancialadjustment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;