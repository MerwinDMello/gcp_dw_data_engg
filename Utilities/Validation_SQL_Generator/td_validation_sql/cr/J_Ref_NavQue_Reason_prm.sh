export JOBNAME='J_Ref_NavQue_Reason'

export AC_EXP_SQL_STATEMENT="select 'J_Ref_NavQue_Reason'+ ',' +
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from  [Navadhoc].[dbo].[DimNavQReason] (NOLOCK)"

export AC_ACT_SQL_STATEMENT="select 'J_Ref_NavQue_Reason' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_NavQue_Reason
where DW_Last_Update_Date_Time>= (Select max(Job_Start_Date_Time) from EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name='J_Ref_NavQue_Reason' and Job_Status_Code is NULL)"