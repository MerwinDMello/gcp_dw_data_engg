
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBDCR002-20'
export AC_ACT_SQL_STATEMENT="SELECT 'PBDCR002-20' || ',' || cast(zeroifnull(Count(*)) as varchar(20)) || ','  || cast(zeroifnull(SUM(Refund_Amt)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs.Payment_Compliance_Credit_Inventory CBD where Reporting_Date = current_date and Credit_Balance_Refund_Ind = 'R';"

export AC_EXP_SQL_STATEMENT="SELECT 'PBDCR002-20' || ',' || cast(zeroifnull(Count(*)) as varchar(20)) || ',' || cast(zeroifnull(SUM(Refund_Amt)) as varchar(20)) || ',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Refund_Transmitted Stg ;"




