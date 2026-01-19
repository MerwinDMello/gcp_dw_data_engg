CREATE OR REPLACE VIEW edwpsc_views.`ecw_fact53rdpatientpayment`
AS SELECT
  `ecw_fact53rdpatientpayment`.patientpaymentkey,
  `ecw_fact53rdpatientpayment`.accountnumber,
  `ecw_fact53rdpatientpayment`.patientname,
  `ecw_fact53rdpatientpayment`.ocrscanline,
  `ecw_fact53rdpatientpayment`.statementdate,
  `ecw_fact53rdpatientpayment`.coid,
  `ecw_fact53rdpatientpayment`.practiceid,
  `ecw_fact53rdpatientpayment`.paymentdate,
  `ecw_fact53rdpatientpayment`.paymenttypeflag,
  `ecw_fact53rdpatientpayment`.paymenttype,
  `ecw_fact53rdpatientpayment`.creditcardtypeflag,
  `ecw_fact53rdpatientpayment`.creditcardtype,
  `ecw_fact53rdpatientpayment`.checknumber,
  `ecw_fact53rdpatientpayment`.paidamount,
  `ecw_fact53rdpatientpayment`.balancedue,
  `ecw_fact53rdpatientpayment`.sourcefilename,
  `ecw_fact53rdpatientpayment`.insertedby,
  `ecw_fact53rdpatientpayment`.inserteddtm,
  `ecw_fact53rdpatientpayment`.modifiedby,
  `ecw_fact53rdpatientpayment`.modifieddtm,
  `ecw_fact53rdpatientpayment`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_fact53rdpatientpayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_fact53rdpatientpayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;