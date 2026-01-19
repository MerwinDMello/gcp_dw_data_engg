CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtclaimsummarymodifierpk`
AS SELECT
  `ccu_rprtclaimsummarymodifierpk`.systemname,
  `ccu_rprtclaimsummarymodifierpk`.dosprovidername,
  `ccu_rprtclaimsummarymodifierpk`.regionname,
  `ccu_rprtclaimsummarymodifierpk`.claimnumber,
  `ccu_rprtclaimsummarymodifierpk`.encounterid,
  `ccu_rprtclaimsummarymodifierpk`.encountercount,
  `ccu_rprtclaimsummarymodifierpk`.billingarea,
  `ccu_rprtclaimsummarymodifierpk`.coidspecialty,
  `ccu_rprtclaimsummarymodifierpk`.coid,
  `ccu_rprtclaimsummarymodifierpk`.claimcoid,
  `ccu_rprtclaimsummarymodifierpk`.regionkey,
  `ccu_rprtclaimsummarymodifierpk`.encounterdate,
  `ccu_rprtclaimsummarymodifierpk`.emrmodifiervsclaimmodifier,
  `ccu_rprtclaimsummarymodifierpk`.modifiercodechange,
  `ccu_rprtclaimsummarymodifierpk`.encountermodifier,
  `ccu_rprtclaimsummarymodifierpk`.claimmodifier,
  `ccu_rprtclaimsummarymodifierpk`.lastchangedby34,
  `ccu_rprtclaimsummarymodifierpk`.lastchangedbydept,
  `ccu_rprtclaimsummarymodifierpk`.billingnotes,
  `ccu_rprtclaimsummarymodifierpk`.patientaccountnumber,
  `ccu_rprtclaimsummarymodifierpk`.financialnumber,
  `ccu_rprtclaimsummarymodifierpk`.patientname,
  `ccu_rprtclaimsummarymodifierpk`.encounterdateyyyymm,
  `ccu_rprtclaimsummarymodifierpk`.lasttoucheddate_yyyymm,
  `ccu_rprtclaimsummarymodifierpk`.encountercoidlob,
  `ccu_rprtclaimsummarymodifierpk`.encountercoidsublob,
  `ccu_rprtclaimsummarymodifierpk`.dwlastupdateddate,
  `ccu_rprtclaimsummarymodifierpk`.lasttoucheddate,
  `ccu_rprtclaimsummarymodifierpk`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtclaimsummarymodifierpk`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtclaimsummarymodifierpk`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;