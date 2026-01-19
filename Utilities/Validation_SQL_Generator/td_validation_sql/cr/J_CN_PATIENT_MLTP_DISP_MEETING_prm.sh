export JOBNAME='J_CN_PATIENT_MLTP_DISP_MEETING'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_MLTP_DISP_MEETING'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Mltp_Disc_Meet_SID,
      Nav_Patient_Id,
      Tumor_Type_Id,
      Navigator_Id,
      Coid,
      Company_Code,
      Meeting_Date,
      Patient_Discussed_Ind,
      Treatment_Change_Ind,
      Meeting_Notes_Text,
      Hashbite_SSK,
      Source_System_Code,
      DW_Last_Update_Date_Time
From $NCR_STG_SCHEMA.CN_Patient_Mltp_Disciplinary_Meeting_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Mltp_Disciplinary_Meeting)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_MLTP_DISP_MEETING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_Patient_Mltp_Disciplinary_Meeting  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_MLTP_DISP_MEETING');"
