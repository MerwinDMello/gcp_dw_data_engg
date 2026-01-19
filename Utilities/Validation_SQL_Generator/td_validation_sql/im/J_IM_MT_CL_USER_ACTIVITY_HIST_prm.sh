#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_MT_CL_USER_ACTIVITY_HIST'
export JOBNAME='J_IM_MT_CL_USER_ACTIVITY_HIST'

export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_CL_USER_ACTIVITY_HIST' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
SELECT DISTINCT Network_Mnemonic_CS, MT_User_Mnemonic_CS FROM Edwim_Staging.MT_CL_User_Activity 
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_CL_USER_ACTIVITY_HIST' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CL_User_Activity_Hist    
WHERE DW_Last_Update_Date_Time > (SELECT MAX(Job_Start_Date_Time) FROM EDWIM_DMX_AC.ETL_JOB_RUN WHERE job_name = 'J_IM_MT_CL_USER_ACTIVITY_HIST' AND Job_Status_Code IS NULL)
)A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#

