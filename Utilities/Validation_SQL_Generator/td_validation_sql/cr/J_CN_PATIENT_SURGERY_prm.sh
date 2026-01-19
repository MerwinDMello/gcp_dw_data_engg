export JOBNAME='J_CN_PATIENT_SURGERY'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_SURGERY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Surgery_SID,
      RS.Side_ID,
      RF.Facility_Id,
      Surgery_Type_Id,
      Core_Record_Type_Id,
      Med_Spcl_Physician_Id,
      Referring_Physician_Id,
      Nav_Patient_Id,
      Tumor_Type_Id,
      Diagnosis_Result_Id,
      Nav_Diagnosis_Id,
      Navigator_Id,
      Coid,
      Company_Code,
      Surgery_Date,
      General_Surgery_Type_Text,
      Reconstructive_Offered_Ind,
      Palliative_Ind,
      Comment_Text,
      Hashbite_SSK,
      STG.Source_System_Code,
      STG.DW_Last_Update_Date_Time
From $NCR_STG_SCHEMA.CN_PATIENT_SURGERY_STG STG 
LEFT JOIN $NCR_TGT_SCHEMA.Ref_Side RS ON Trim(STG.SurgerySide) = Trim(RS.Side_Desc)
LEFT JOIN $NCR_TGT_SCHEMA.Ref_Facility RF ON Trim(STG.SurgeryFacility) = Trim(RF.facility_Name)
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_PATIENT_SURGERY)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_SURGERY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_PATIENT_SURGERY  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_SURGERY');"
