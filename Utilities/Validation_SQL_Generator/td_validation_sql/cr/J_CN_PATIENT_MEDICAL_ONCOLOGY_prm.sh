export JOBNAME='J_CN_PATIENT_MEDICAL_ONCOLOGY'


export AC_EXP_SQL_STATEMENT="
SELECT	'J_CN_PATIENT_MEDICAL_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
stg.CN_Patient_Medical_Oncology_ID
,stg.Treatment_Type_Id 
,ref.Facility_Id
,stg.Core_Record_Type_Id
,stg.Med_Spcl_Physician_Id
,stg.Nav_Patient_Id
,stg.Tumor_Type_Id
,stg.Diagnosis_Result_Id
,stg.Nav_Diagnosis_Id
,stg.Navigator_Id
,stg.Coid
,stg.Company_Code
,stg.Core_Record_Date
,stg.Treatment_Start_Date
,stg.Treatment_End_Date
,stg.Estimated_End_Date
,stg.Drug_Name
,stg.Dose_Dense_Chemo_Ind
,stg.Drug_Dose_Amt_Text
,stg.Drug_Dose_Measurement_Text
,stg.Drug_Available_Ind
,stg.Drug_Qty
,stg.Cycle_Num
,stg.Cycle_Frequency_Text
,stg.Medical_Oncology_Reason_Text
,stg.Terminated_Ind
,stg.Treatment_Therapy_Schedule_Cd
,stg.Comment_Text
,stg.Hashbite_SSK
,stg.Source_System_Code 
,Current_timestamp(0) as DW_Last_Update_Date_Time

from $NCR_STG_SCHEMA.CN_PATIENT_MEDICAL_ONCOLOGY_Stg stg
left  join EDWCR.Ref_Facility ref
ON stg.Medical_Oncology_Facility_Id = Ref.Facility_Name
 
where Hashbite_SSK  not in (Select Hashbite_SSK from $EDWCR_BASE_VIEWS.CN_PATIENT_MEDICAL_ONCOLOGY )

) A;"


export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_MEDICAL_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_PATIENT_MEDICAL_ONCOLOGY WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_MEDICAL_ONCOLOGY');"



