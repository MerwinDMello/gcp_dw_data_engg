CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtclaimsummarymodifierecw`
AS SELECT
  `ccu_rprtclaimsummarymodifierecw`.systemname,
  `ccu_rprtclaimsummarymodifierecw`.dosprovidername,
  `ccu_rprtclaimsummarymodifierecw`.regionname,
  `ccu_rprtclaimsummarymodifierecw`.claimnumber,
  `ccu_rprtclaimsummarymodifierecw`.encounterid,
  `ccu_rprtclaimsummarymodifierecw`.encountercount,
  `ccu_rprtclaimsummarymodifierecw`.billingarea,
  `ccu_rprtclaimsummarymodifierecw`.coidspecialty,
  `ccu_rprtclaimsummarymodifierecw`.coid,
  `ccu_rprtclaimsummarymodifierecw`.claimcoid,
  `ccu_rprtclaimsummarymodifierecw`.regionkey,
  `ccu_rprtclaimsummarymodifierecw`.encounterdate,
  `ccu_rprtclaimsummarymodifierecw`.emrmodifiervsclaimmodifier,
  `ccu_rprtclaimsummarymodifierecw`.modifiercodechange,
  `ccu_rprtclaimsummarymodifierecw`.encountermodifier,
  `ccu_rprtclaimsummarymodifierecw`.claimmodifier,
  `ccu_rprtclaimsummarymodifierecw`.lastchangedby34,
  `ccu_rprtclaimsummarymodifierecw`.lastchangedbydept,
  `ccu_rprtclaimsummarymodifierecw`.billingnotes,
  `ccu_rprtclaimsummarymodifierecw`.patientaccountnumber,
  `ccu_rprtclaimsummarymodifierecw`.financialnumber,
  `ccu_rprtclaimsummarymodifierecw`.patientname,
  `ccu_rprtclaimsummarymodifierecw`.encounterdateyyyymm,
  `ccu_rprtclaimsummarymodifierecw`.lasttoucheddate_yyyymm,
  `ccu_rprtclaimsummarymodifierecw`.encountercoidlob,
  `ccu_rprtclaimsummarymodifierecw`.encountercoidsublob,
  `ccu_rprtclaimsummarymodifierecw`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtclaimsummarymodifierecw`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtclaimsummarymodifierecw`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;