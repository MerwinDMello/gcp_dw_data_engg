export JOBNAME='J_CN_PATIENT_SURV_PLAN_TASK'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_SURV_PLAN_TASK'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      Nav_Survivorship_Plan_Task_SID,
      REF2.Status_ID ,
      REF1.Contact_Method_ID,
      Nav_Patient_Id ,
      Navigator_Id ,
      Coid ,
      Company_Code ,
      Task_Desc_Text, 
      Task_Resolution_Date ,
      Task_Closed_Date ,
      Contact_Result_Text, 
      Contact_Date ,
      Comment_Text, 
      Hashbite_SSK ,
      STG.Source_System_Code ,
      STG.DW_Last_Update_Date_Time 
From $NCR_STG_SCHEMA.CN_Patient_Survivorship_Plan_Task_STG STG 
LEFT OUTER JOIN $NCR_TGT_SCHEMA.REF_CONTACT_METHOD REF1 ON 
STG.TaskMeasnOfContact = REF1.Contact_Method_Desc
LEFT OUTER JOIN $NCR_TGT_SCHEMA.REF_STATUS REF2 ON 
STG.TASKSTATE = REF2.Status_Desc
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Survivorship_Plan_Task)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_SURV_PLAN_TASK'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_Patient_Survivorship_Plan_Task  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_SURV_PLAN_TASK');"
