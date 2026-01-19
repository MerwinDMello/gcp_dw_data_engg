CREATE OR REPLACE VIEW edwpsc_views.`ecw_cashvaluepatientpaymenthistory`
AS SELECT
  `ecw_cashvaluepatientpaymenthistory`.patientbilltype,
  `ecw_cashvaluepatientpaymenthistory`.coid,
  `ecw_cashvaluepatientpaymenthistory`.deptcode,
  `ecw_cashvaluepatientpaymenthistory`.marketkey,
  `ecw_cashvaluepatientpaymenthistory`.specialtycategorykey,
  `ecw_cashvaluepatientpaymenthistory`.renderingproviderkey,
  `ecw_cashvaluepatientpaymenthistory`.guarantorpatientkey,
  `ecw_cashvaluepatientpaymenthistory`.claimkey,
  `ecw_cashvaluepatientpaymenthistory`.firstpatientstatementbilldatekey,
  `ecw_cashvaluepatientpaymenthistory`.lastpatientstatementbilldatekey,
  `ecw_cashvaluepatientpaymenthistory`.numberofpatientstatements,
  `ecw_cashvaluepatientpaymenthistory`.lastpatientclaimpaymentdatekey,
  `ecw_cashvaluepatientpaymenthistory`.bankdaystopayment,
  `ecw_cashvaluepatientpaymenthistory`.totalpatientresponsibilityamt,
  `ecw_cashvaluepatientpaymenthistory`.balancebilledtopatient,
  `ecw_cashvaluepatientpaymenthistory`.upfrontpaymentamt,
  `ecw_cashvaluepatientpaymenthistory`.postbillpaymentamt,
  `ecw_cashvaluepatientpaymenthistory`.totalpatientpaymentsamt,
  `ecw_cashvaluepatientpaymenthistory`.totalpatientbalanceamt,
  `ecw_cashvaluepatientpaymenthistory`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_cashvaluepatientpaymenthistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_cashvaluepatientpaymenthistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;