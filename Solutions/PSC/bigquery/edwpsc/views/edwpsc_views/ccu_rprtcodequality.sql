CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtcodequality`
AS SELECT
  `ccu_rprtcodequality`.codequalitykey,
  `ccu_rprtcodequality`.claimkey,
  `ccu_rprtcodequality`.claimnumber,
  `ccu_rprtcodequality`.regionkey,
  `ccu_rprtcodequality`.coid,
  `ccu_rprtcodequality`.servicedatekey,
  `ccu_rprtcodequality`.svcproviderspecialty,
  `ccu_rprtcodequality`.firstinsbilldatewkofmonth,
  `ccu_rprtcodequality`.firstdenialcategories,
  `ccu_rprtcodequality`.prebilleditcategories,
  `ccu_rprtcodequality`.qualitycategory,
  `ccu_rprtcodequality`.coder,
  `ccu_rprtcodequality`.coder34id,
  `ccu_rprtcodequality`.codertype,
  `ccu_rprtcodequality`.claimcount,
  `ccu_rprtcodequality`.practiceid,
  `ccu_rprtcodequality`.patientmrn,
  `ccu_rprtcodequality`.patientinternalid,
  `ccu_rprtcodequality`.visitnumber,
  `ccu_rprtcodequality`.transactionnumber_combined,
  `ccu_rprtcodequality`.sourcesystemcode,
  `ccu_rprtcodequality`.dwlastupdatedatetime,
  `ccu_rprtcodequality`.insertedby,
  `ccu_rprtcodequality`.inserteddtm,
  `ccu_rprtcodequality`.modifiedby,
  `ccu_rprtcodequality`.modifieddtm,
  `ccu_rprtcodequality`.coderactiondate,
  `ccu_rprtcodequality`.firstinsurancebilldate,
  `ccu_rprtcodequality`.minfirstdenialeradate
  FROM
    edwpsc_base_views.`ccu_rprtcodequality`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtcodequality`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;