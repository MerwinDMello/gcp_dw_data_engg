export JOBNAME='J_CN_PATIENT_RADIATION_ONCOLOGY'


export AC_EXP_SQL_STATEMENT="
SELECT	'J_CN_PATIENT_RADIATION_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
stg.CN_Patient_Rad_Oncology_SID
,loc.Site_Location_Id
,stg.Treatment_Type_Id
,lob_loc.Lung_Lobe_Location_Id
,fac.Facility_Id
,stg.Nav_Patient_Id
,stg.Core_Record_Type_Id
,stg.Med_Spcl_Physician_Id
,stg.Tumor_Type_Id
,stg.Diagnosis_Result_Id
,stg.Nav_Diagnosis_Id
,stg.Navigator_Id
,stg.Coid
,stg.Company_Code
,stg.Core_Record_Date
,stg.Treatment_Start_Date
,stg.Treatment_End_Date
,stg.Treatment_Fractions_Num
,stg.Elapse_Ind
,stg.Elapse_Start_Date
,stg.Elapse_End_Date
,stg.Radiation_Oncology_Reason_Text
,stg.Palliative_Ind
,stg.Treatment_Therapy_Schedule_Cd
,stg.Comment_Text
,stg.Hashbite_SSK
,stg.Source_System_Code
,Current_timestamp(0) as DW_Last_Update_Date_Time

from $NCR_STG_SCHEMA.CN_PATIENT_RADIATION_ONCOLOGY_STG stg
left join edwcr.Ref_Site_Location loc
ON stg.Treatment_Site_Location_Id=loc.Site_Location_Desc

Left join edwcr.Ref_lung_lobe_location lob_loc
ON stg.Lung_Lobe_Location_Id = lob_loc.Lung_Lobe_Location_Desc

LEFT JOIN edwcr.Ref_Facility fac
ON stg.Radiation_Oncology_Facility_Id = fac.Facility_Name

where Hashbite_SSK  not in (Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_PATIENT_RADIATION_ONCOLOGY )
) A;"


export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_RADIATION_ONCOLOGY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_PATIENT_RADIATION_ONCOLOGY WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_RADIATION_ONCOLOGY');"



