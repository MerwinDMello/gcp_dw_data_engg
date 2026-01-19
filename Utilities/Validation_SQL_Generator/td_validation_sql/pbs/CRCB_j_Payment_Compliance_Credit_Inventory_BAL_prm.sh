
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBDCR002-40'
export AC_ACT_SQL_STATEMENT="SELECT 'PBDCR002-40' || ',' || cast(zeroifnull(Count(*)) as varchar(20)) || ','  || cast(zeroifnull(SUM(Refund_Amt)) as varchar(20)) || ','  || cast(zeroifnull(SUM(Total_Account_Balance_Amt)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs.Payment_Compliance_Credit_Inventory CBD where Reporting_Date = current_date and Credit_Balance_Refund_Ind = 'B';"

export AC_EXP_SQL_STATEMENT="SELECT 'PBDCR002-40' || ',' || cast(zeroifnull(Count(*)) as varchar(20)) || ',' || cast(zeroifnull(SUM(Refund_Amt)) as varchar(20)) || ',' || cast(zeroifnull(SUM(Total_Acct_Bal_Amt)) as varchar(20)) ||',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Balance_Refund_Header Stg;"




