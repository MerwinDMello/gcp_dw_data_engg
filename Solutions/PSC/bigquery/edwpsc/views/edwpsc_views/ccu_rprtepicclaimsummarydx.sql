CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtepicclaimsummarydx`
AS SELECT
  `ccu_rprtepicclaimsummarydx`.systemname,
  `ccu_rprtepicclaimsummarydx`.regionname,
  `ccu_rprtepicclaimsummarydx`.dosprovidername,
  `ccu_rprtepicclaimsummarydx`.providerspeciality,
  `ccu_rprtepicclaimsummarydx`.dosprovideruname,
  `ccu_rprtepicclaimsummarydx`.claimnumber,
  `ccu_rprtepicclaimsummarydx`.encounterid,
  `ccu_rprtepicclaimsummarydx`.encountercount,
  `ccu_rprtepicclaimsummarydx`.billingarea,
  `ccu_rprtepicclaimsummarydx`.coidspecialty,
  `ccu_rprtepicclaimsummarydx`.coid,
  `ccu_rprtepicclaimsummarydx`.claimcoid,
  `ccu_rprtepicclaimsummarydx`.regionkey,
  `ccu_rprtepicclaimsummarydx`.encounterdate,
  `ccu_rprtepicclaimsummarydx`.lasttoucheddateyyyymm,
  `ccu_rprtepicclaimsummarydx`.emrdxsvsclaimdxs,
  `ccu_rprtepicclaimsummarydx`.dxcodechange,
  `ccu_rprtepicclaimsummarydx`.encounterdxcode,
  `ccu_rprtepicclaimsummarydx`.claimdxcode,
  `ccu_rprtepicclaimsummarydx`.lastchangedby34dept,
  `ccu_rprtepicclaimsummarydx`.billingnotes,
  `ccu_rprtepicclaimsummarydx`.patientaccountnumber,
  `ccu_rprtepicclaimsummarydx`.financialnumber,
  `ccu_rprtepicclaimsummarydx`.patientname,
  `ccu_rprtepicclaimsummarydx`.encounterdateyyyymm,
  `ccu_rprtepicclaimsummarydx`.encountercoidlob,
  `ccu_rprtepicclaimsummarydx`.encountercoidsublob,
  `ccu_rprtepicclaimsummarydx`.dwlastupdatedate,
  `ccu_rprtepicclaimsummarydx`.visitnumber,
  `ccu_rprtepicclaimsummarydx`.transactionnumbercombined,
  `ccu_rprtepicclaimsummarydx`.patientinternalidcombined,
  `ccu_rprtepicclaimsummarydx`.lasttoucheddate,
  `ccu_rprtepicclaimsummarydx`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ccu_rprtepicclaimsummarydx`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtepicclaimsummarydx`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;