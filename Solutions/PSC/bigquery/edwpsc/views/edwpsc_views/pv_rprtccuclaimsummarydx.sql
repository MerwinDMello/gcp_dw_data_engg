CREATE OR REPLACE VIEW edwpsc_views.`pv_rprtccuclaimsummarydx`
AS SELECT
  `pv_rprtccuclaimsummarydx`.systemname,
  `pv_rprtccuclaimsummarydx`.dosprovidername,
  `pv_rprtccuclaimsummarydx`.claimnumber,
  `pv_rprtccuclaimsummarydx`.encounterid,
  `pv_rprtccuclaimsummarydx`.encountercount,
  `pv_rprtccuclaimsummarydx`.billingarea,
  `pv_rprtccuclaimsummarydx`.coidspecialty,
  `pv_rprtccuclaimsummarydx`.coid,
  `pv_rprtccuclaimsummarydx`.claimcoid,
  `pv_rprtccuclaimsummarydx`.regionkey,
  `pv_rprtccuclaimsummarydx`.encounterdate,
  `pv_rprtccuclaimsummarydx`.emrdxsvsclaimdxs,
  `pv_rprtccuclaimsummarydx`.dxcodechange,
  `pv_rprtccuclaimsummarydx`.encounterdxcode,
  `pv_rprtccuclaimsummarydx`.claimdxcode,
  `pv_rprtccuclaimsummarydx`.lastchangedby34dept,
  `pv_rprtccuclaimsummarydx`.billingnotes,
  `pv_rprtccuclaimsummarydx`.patientaccountnumber,
  `pv_rprtccuclaimsummarydx`.financialnumber,
  `pv_rprtccuclaimsummarydx`.patientname,
  `pv_rprtccuclaimsummarydx`.encounterdateyyyymm,
  `pv_rprtccuclaimsummarydx`.lasttoucheddate_yyyymm,
  `pv_rprtccuclaimsummarydx`.encountercoidlob,
  `pv_rprtccuclaimsummarydx`.encountercoidsublob,
  `pv_rprtccuclaimsummarydx`.dwlastupdatedatetime,
  `pv_rprtccuclaimsummarydx`.encountertype,
  `pv_rprtccuclaimsummarydx`.practicename,
  `pv_rprtccuclaimsummarydx`.pvwcclaimnumbercombined,
  `pv_rprtccuclaimsummarydx`.lasttoucheddate,
  `pv_rprtccuclaimsummarydx`.ccuclaimsummarydxkey
  FROM
    edwpsc_base_views.`pv_rprtccuclaimsummarydx`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_rprtccuclaimsummarydx`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;