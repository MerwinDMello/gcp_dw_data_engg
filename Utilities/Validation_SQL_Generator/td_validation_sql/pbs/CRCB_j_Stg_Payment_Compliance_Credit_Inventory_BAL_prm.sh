
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBDCR002-30'
export AC_ACT_SQL_STATEMENT="Locking table  EDWPBS_Staging.Stg_CR_Balance_Refund_Header for access
SELECT 'PBDCR002-30' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Balance_Refund_Header Stg"

export AC_EXP_SQL_STATEMENT="Select 'PBDCR002-30' +','+ ltrim(rtrim(cast(count(1) as Varchar(20)))) +',' as Source_String from (
select 
HeaderID, 
UnitNum,
cast(PatNum as Decimal(18)) as Pat_Acct_Num,
FinClass, 
CASE WHEN FinCLass in (1,2,3,6) THEN 'Government' ELSE 'Managed Care' END AS PayerCategory,
TotAccountBal,
TotCharge, 
TotCashPay, 
TotAllow, 
TotPolicyAdj, 
TotWriteOff, 
Ins1AA1Plan, 
Ins2AA1Plan, 
Ins3AA1Plan, 
AcctStatus,
AA6AdmitDate,
AA7DischDate,
RefundType,
case 
   when RefundType = 1 then 'Primary Insurance Refund' 
   when RefundType = 2 then 'Secondary/Tertiary Refund' 
   when RefundType = 3 then 'Patient Refund' 
end as RefundTypeDesc, 
RefundAmount, 
MemberID,
RefundInsIPlan, 
DateLastActivity, 
RefName,
LastUpdate, 
LastUpdateBy, 
cast(RefSpecial as char(6)) as RefundSpecial,
RefProcCode,
RefGLAcct,
Logged,
EnteredDate,
[Status],
dtmApproved, 
ApprovedBy,  
TransmittedDate, 
PatientType, 
BillThroughDate, 
AutoRefundCreatedDate, 
curGRVAmt,  
dtFinalBilled, 
blnEligibleTransfer,
blnLateCharge,
dtRefundCreated, 
strRefundCreatedUser, 
RestoreDate,  
RestoreIndicator,
case when blnEligibleTransfer = '1' then 'Y' else 'N' end as EligibleTransferInd,
case when blnLateCharge = '1' then 'Y' else 'N' end as LateChargeInd,
AdrName,
AdrAddr1,
AdrAddr2,
AdrState,
AdrCityState,
AdrZip,
AdrCountry,
RefundReason,
RefAddr1,
RefAddr2,
RefCity,
RefState,
RefZip,
RefCountry,
case when Logged = '1' then 'Y' else 'N' end as LoggedInd,
CreatedDate
from RcomCreditRefund.dbo.tblheader a
Where TotAccountBal < 0
)Src"




