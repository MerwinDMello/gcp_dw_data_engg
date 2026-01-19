
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_Stg_HPS_Pat_Payment_Link_Dtl'


export AC_EXP_SQL_STATEMENT="select 'J_Stg_HPS_Pat_Payment_Link_Dtl' + ',' + CAST(COUNT(*) AS VARCHAR(20)) + ',' as Source_String from [HealthcarePaymentSystem].[dbo].[HPSReporting_PaymentLink]"

export AC_ACT_SQL_STATEMENT="select 'J_Stg_HPS_Pat_Payment_Link_Dtl' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from EDWPBS_Staging.Stg_HPS_Pat_Payment_Link_Dtl"
#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
