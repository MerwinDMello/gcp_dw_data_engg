CREATE OR REPLACE VIEW edwpsc_views.`onbase_factworkqueue`
AS SELECT
  `onbase_factworkqueue`.documenthandle,
  `onbase_factworkqueue`.itemname,
  `onbase_factworkqueue`.lifecyclename,
  `onbase_factworkqueue`.documenttype,
  `onbase_factworkqueue`.queuename,
  `onbase_factworkqueue`.entrydate,
  `onbase_factworkqueue`.entrytime,
  `onbase_factworkqueue`.exitdate,
  `onbase_factworkqueue`.exittime,
  `onbase_factworkqueue`.username,
  `onbase_factworkqueue`.userid,
  `onbase_factworkqueue`.providerlastname,
  `onbase_factworkqueue`.providerfirstname,
  `onbase_factworkqueue`.color,
  `onbase_factworkqueue`.npi,
  `onbase_factworkqueue`.decision,
  `onbase_factworkqueue`.status,
  `onbase_factworkqueue`.packettype,
  `onbase_factworkqueue`.effectivedate,
  `onbase_factworkqueue`.numberofdaysopen,
  `onbase_factworkqueue`.completeflag,
  `onbase_factworkqueue`.insertedby,
  `onbase_factworkqueue`.inserteddtm,
  `onbase_factworkqueue`.modifiedby,
  `onbase_factworkqueue`.modifieddtm,
  `onbase_factworkqueue`.arqueue,
  `onbase_factworkqueue`.urgency,
  `onbase_factworkqueue`.payor,
  `onbase_factworkqueue`.startdate,
  `onbase_factworkqueue`.datestored,
  `onbase_factworkqueue`.itemstatus,
  `onbase_factworkqueue`.patientname,
  `onbase_factworkqueue`.wfscrub1,
  `onbase_factworkqueue`.wfscrub2,
  `onbase_factworkqueue`.coid,
  `onbase_factworkqueue`.erabatchnumber,
  `onbase_factworkqueue`.cliniccode,
  `onbase_factworkqueue`.treasurybatchnumber,
  `onbase_factworkqueue`.dwlastupdatedatetime,
  `onbase_factworkqueue`.onbaseworkqueuekey
  FROM
    edwpsc_base_views.`onbase_factworkqueue`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`onbase_factworkqueue`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;