export JOBNAME='J_CN_PATIENT_BIOPSY'
export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_BIOPSY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      CN_Patient_Biopsy_SID,
      Core_Record_Type_Id,
      Med_Spcl_Physician_Id ,
      Referring_Physician_Id ,
      Biopsy_Type_Id ,
      Biopsy_Result_Id ,
      FAC.Facility_Id,
      L.Site_Location_Id ,
      S.Physician_Specialty_Id,
      Nav_Patient_Id ,
      Tumor_Type_Id ,
      Diagnosis_Result_Id ,
      Nav_Diagnosis_Id ,
      Navigator_Id ,
      Coid ,
      Company_Code ,
      Biopsy_Date ,
      Biopsy_Clip_Sw ,
      Biopsy_Needle_Sw ,
      General_Biopsy_Type_Text ,
      Comment_Text ,
      Hashbite_SSK ,
      STG.Source_System_Code ,
      STG.DW_Last_Update_Date_Time 
from  $NCR_STG_SCHEMA.CN_PATIENT_BIOPSY_STG STG
LEFT JOIN $EDWCR_BASE_VIEWS.REF_FACILITY FAC ON STG.BiopsyFacility = FAC.Facility_Name
LEFT JOIN $EDWCR_BASE_VIEWS.Ref_Site_Location L ON STG.BiopsySite = L.Site_Location_Desc
LEFT JOIN $EDWCR_BASE_VIEWS.Ref_Physician_Specialty S ON STG.BiopsyPhysicianType = S.Physician_Specialty_Desc
where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $EDWCR_BASE_VIEWS.CN_PATIENT_BIOPSY)
)A;"
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_BIOPSY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_PATIENT_BIOPSY  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_BIOPSY');"
