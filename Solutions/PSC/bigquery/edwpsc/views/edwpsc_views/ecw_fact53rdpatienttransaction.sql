CREATE OR REPLACE VIEW edwpsc_views.`ecw_fact53rdpatienttransaction`
AS SELECT
  `ecw_fact53rdpatienttransaction`.patienttransactionkey,
  `ecw_fact53rdpatienttransaction`.sourceprimarykeyvalue,
  `ecw_fact53rdpatienttransaction`.transactiontype,
  `ecw_fact53rdpatienttransaction`.method,
  `ecw_fact53rdpatienttransaction`.channel,
  `ecw_fact53rdpatienttransaction`.biller,
  `ecw_fact53rdpatienttransaction`.patientname,
  `ecw_fact53rdpatienttransaction`.practicename,
  `ecw_fact53rdpatienttransaction`.patientkey,
  `ecw_fact53rdpatienttransaction`.patientaccountnumber,
  `ecw_fact53rdpatienttransaction`.confirmationnumber,
  `ecw_fact53rdpatienttransaction`.paymentcreateddate,
  `ecw_fact53rdpatienttransaction`.paymentdate,
  `ecw_fact53rdpatienttransaction`.accounttype,
  `ecw_fact53rdpatienttransaction`.totalpaymentamount,
  `ecw_fact53rdpatienttransaction`.paymentstatus,
  `ecw_fact53rdpatienttransaction`.paymenttime,
  `ecw_fact53rdpatienttransaction`.settledtime,
  `ecw_fact53rdpatienttransaction`.addressline1,
  `ecw_fact53rdpatienttransaction`.zip,
  `ecw_fact53rdpatienttransaction`.pmtaccounttype,
  `ecw_fact53rdpatienttransaction`.lastfourdigits,
  `ecw_fact53rdpatienttransaction`.avsresponse,
  `ecw_fact53rdpatienttransaction`.cvvresponse,
  `ecw_fact53rdpatienttransaction`.stdentryclass,
  `ecw_fact53rdpatienttransaction`.revreasonorccauth,
  `ecw_fact53rdpatienttransaction`.userid,
  `ecw_fact53rdpatienttransaction`.loginid,
  `ecw_fact53rdpatienttransaction`.customerid,
  `ecw_fact53rdpatienttransaction`.csrrep,
  `ecw_fact53rdpatienttransaction`.coid,
  `ecw_fact53rdpatienttransaction`.billerremittancefield1,
  `ecw_fact53rdpatienttransaction`.billerremittancefield2,
  `ecw_fact53rdpatienttransaction`.billerremittancefield3,
  `ecw_fact53rdpatienttransaction`.billerremittancefield5,
  `ecw_fact53rdpatienttransaction`.sourcefilename,
  `ecw_fact53rdpatienttransaction`.insertedby,
  `ecw_fact53rdpatienttransaction`.inserteddtm,
  `ecw_fact53rdpatienttransaction`.modifiedby,
  `ecw_fact53rdpatienttransaction`.modifieddtm,
  `ecw_fact53rdpatienttransaction`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_fact53rdpatienttransaction`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_fact53rdpatienttransaction`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;