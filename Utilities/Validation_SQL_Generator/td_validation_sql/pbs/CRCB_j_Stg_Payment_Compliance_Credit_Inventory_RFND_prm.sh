
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBDCR002-10'
export AC_ACT_SQL_STATEMENT="Locking table  EDWPBS_Staging.Stg_CR_Refund_Transmitted for access
SELECT 'PBDCR002-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Refund_Transmitted Stg"

export AC_EXP_SQL_STATEMENT="Select 'PBDCR002-10' +','+ ltrim(rtrim(cast(count(1) as Varchar(20)))) +',' as Source_String from (
SELECT HeaderID
      ,UnitNum
      ,cast(PatNum as decimal(12)) as Pat_Acct_Num
      ,TotAccountBal
      ,TotCharge
      ,TotCashPay
      ,TotAllow
      ,TotPolicyAdj
      ,TotWriteOff
      ,FinClass
      ,Ins1AA1Plan
      ,Ins2AA1Plan
      ,Ins3AA1Plan
      ,AcctStatus
      ,AA6AdmitDate
      ,AA7DischDate
      ,AdrName
      ,AdrAddr1
      ,AdrAddr2
      ,AdrCityState
      ,AdrZip
      ,RefundType
      ,case 
         when RefundType = 1 then 'Primary Insurance Refund' 
         when RefundType = 2 then 'Secondary/Tertiary Refund' 
         when RefundType = 3 then 'Patient Refund' 
       end as RefundTypeDesc
      ,RefundAmount
      ,RefundReason
      ,MemberID
      ,AdrState
      ,AdrCountry
      ,RefundInsIPlan
      ,AdrSSN
      ,DateLastActivity
      ,RefName
      ,RefAddr1
      ,RefAddr2
      ,RefCity
      ,RefState
      ,RefZip
      ,RefCountry
      ,LastUpdate
      ,LastUpdateBy
      ,RefSpecial
      ,RefProcCode
      ,RefGLAcct
      ,Logged
      ,EnteredDate
      ,Status
      ,dtmApproved
      ,ApprovedBy
      ,TransmittedDate
      ,PatientType
      ,BillThroughDate
      ,AutoRefundCreatedDate
      ,curGRVAmt
      ,dtFinalBilled
      ,blnEligibleTransfer
      ,blnLateCharge
      ,dtRefundCreated
      ,strRefundCreatedUser
      ,RestoreDate
      ,RestoreIndicator
      ,TotPlines
      ,CASE WHEN FinCLass in (1,2,3,6) THEN 'Government' ELSE 'Managed Care' END AS PayerCategory
      ,case when refSpecial = 0 then 'N' else 'Y' end as Ref_Special
      ,case when Logged = 0 then 'N' else 'Y' end as Logged_Ind
      ,case when blnEligibleTransfer = 0 then 'N' else 'Y' end as Eligible_Transfer_Ind
      ,case when blnLateCharge = 0 then 'N' else 'Y' end as Late_Charge_Ind
FROM RcomCreditRefund.dbo.tblTransmittedHeaders
WHERE RefundType in (1,2,3) and (convert(char(10),TransmittedDate,126))=  (convert(char(10), (GETDATE()-1),126)))Src"




