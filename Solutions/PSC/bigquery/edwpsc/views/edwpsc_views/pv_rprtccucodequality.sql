CREATE OR REPLACE VIEW edwpsc_views.`pv_rprtccucodequality`
AS SELECT
  `pv_rprtccucodequality`.codequalitykey,
  `pv_rprtccucodequality`.claimkey,
  `pv_rprtccucodequality`.claimnumber,
  `pv_rprtccucodequality`.regionkey,
  `pv_rprtccucodequality`.coid,
  `pv_rprtccucodequality`.servicedatekey,
  `pv_rprtccucodequality`.svcproviderspecialty,
  `pv_rprtccucodequality`.firstinsbilldatewkofmonth,
  `pv_rprtccucodequality`.firstdenialcategories,
  `pv_rprtccucodequality`.prebilleditcategories,
  `pv_rprtccucodequality`.qualitycategory,
  `pv_rprtccucodequality`.coder,
  `pv_rprtccucodequality`.coder34id,
  `pv_rprtccucodequality`.codertype,
  `pv_rprtccucodequality`.claimcount,
  `pv_rprtccucodequality`.practiceid,
  `pv_rprtccucodequality`.patientmrn,
  `pv_rprtccucodequality`.patientinternalid,
  `pv_rprtccucodequality`.visitnumber,
  `pv_rprtccucodequality`.transactionnumber_combined,
  `pv_rprtccucodequality`.sourcesystemcode,
  `pv_rprtccucodequality`.dwlastupdatedatetime,
  `pv_rprtccucodequality`.hashnomatch,
  `pv_rprtccucodequality`.insertedby,
  `pv_rprtccucodequality`.inserteddtm,
  `pv_rprtccucodequality`.modifiedby,
  `pv_rprtccucodequality`.modifieddtm,
  `pv_rprtccucodequality`.firstinsurancebilldate,
  `pv_rprtccucodequality`.minfirstdenialeradate,
  `pv_rprtccucodequality`.coderactiondate
  FROM
    edwpsc_base_views.`pv_rprtccucodequality`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_rprtccucodequality`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;