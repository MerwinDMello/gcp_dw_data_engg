
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMCR350-10'
export AC_ACT_SQL_STATEMENT="Locking table  edwpbs_staging.Stg_Rcom_Pars_CR for access
SELECT 'PBMCR350-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs_staging.Stg_Rcom_Pars_CR ARP"

export AC_EXP_SQL_STATEMENT="Select 'PBMCR350-10' +','+ ltrim(rtrim(cast(count(1) as Varchar(20)))) +',' as Source_String from (
SELECT 
TRANS.HeaderID, 
TRANS.PatNum, 
TRANS.FinClass, 
TRANS.PatientType, 
TRANS.AcctStatus, 
TRANS.UnitNum, 
TRANS.RefundType, 
TRANS.RefundInsIPlan, 
TRANS.RefundAmount, 
TRANS.TotAccountBal, 
TRANS.TotCharge, 
TRANS.TotCashPay, 
TRANS.TotAllow, 
TRANS.TotPolicyAdj, 
TRANS.TotWriteOff, 
TRANS.AA7DischDate, 
TRANS.DateLastActivity, 
TRANS.TransmittedDate 
FROM
rcomCreditRefund.dbo.vw_expEDWTransmitted TRANS
WHERE TRANS.RefundType  <> 0
AND TRANS.TransmittedDate Between 
ltrim(str(year(dateadd(mm,-1,getdate()))))+ '-' + ltrim(str(month(dateadd(mm,-1,getdate())) )) + '-01'
And 
ltrim(str(year(getdate())))+ '-' + ltrim(str(month(getdate()))) + '-01' ) temp"




