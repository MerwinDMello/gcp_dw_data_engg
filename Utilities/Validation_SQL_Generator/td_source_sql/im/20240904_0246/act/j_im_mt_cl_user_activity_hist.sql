select 'J_IM_MT_CL_USER_ACTIVITY_HIST' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CL_User_Activity_Hist    
WHERE DW_Last_Update_Date_Time > (SELECT MAX(Job_Start_Date_Time) FROM EDWIM_DMX_AC.ETL_JOB_RUN WHERE job_name = 'J_IM_MT_CL_USER_ACTIVITY_HIST' AND Job_Status_Code IS NULL)
)A;