CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_cashvaluepatientpaymenthistory`
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
    edwpsc.`ecw_cashvaluepatientpaymenthistory`
;