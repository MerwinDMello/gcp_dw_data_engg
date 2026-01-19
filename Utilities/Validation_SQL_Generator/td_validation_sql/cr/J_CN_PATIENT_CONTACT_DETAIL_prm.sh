export JOBNAME='J_CN_PATIENT_CONTACT_DETAIL'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_CONTACT_DETAIL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Contact_SID,
      Contact_Detail_Measure_Type_Id,
      Contact_Detail_Measure_Val_Txt,
      Hashbite_SSK,
      Source_System_Code,
      DW_Last_Update_Date_Time
From $NCR_STG_SCHEMA.CN_PATIENT_CONTACT_DETAIL_STG STG 
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_PATIENT_CONTACT_DETAIL)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_CONTACT_DETAIL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_PATIENT_CONTACT_DETAIL  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_CONTACT_DETAIL');"
