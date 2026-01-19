#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_CI_Vw_User_StgLoad'

#######Expected Parameter###################
export AC_EXP_INPUT_FILE='SRC_J_MHB_Vw_User_PhoneCalls_Stg.txt'
export P_EXP_Delimiter='|'
export P_EXP_Control_Total_Field='1'
export P_EXP_Number_of_Fields='19'
export P_EXP_Control_Total_Type='1'

##########A/C Parameter#################

export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Vw_User_PhoneCalls_Stg_Load'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM edwci_staging.vwUserPhoneCalls"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
  