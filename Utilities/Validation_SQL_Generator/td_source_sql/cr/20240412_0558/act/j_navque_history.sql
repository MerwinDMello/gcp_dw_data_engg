Select 'J_NavQue_History'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from edwcr.NavQue_History
where DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_NavQue_History');
