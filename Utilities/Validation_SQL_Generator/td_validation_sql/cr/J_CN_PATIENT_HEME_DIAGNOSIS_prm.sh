
export JOBNAME='J_CN_PATIENT_HEME_DIAGNOSIS'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_HEME_DIAGNOSIS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (
Sel iq.* from
(Sel 
Trim(HBSource) as Hashbite_SSK
From EDWCR_Staging.stg_PatientHemeDiagnosis PHD 
Left Outer Join EDWCR_Base_Views.Ref_Status RS
On Trim(PHD.DiseaseStatus)=Trim(RS.Status_Desc)
and Trim(RS.Status_Type_Desc) ='Disease') iq 
Left Outer Join EDWCR_BASE_VIEWS.CN_Patient_Heme_Diagnosis CPHD
on Trim(iq.Hashbite_SSK) = Trim(CPHD.Hashbite_SSK)
where Trim(CPHD.Hashbite_SSK) is null
) iqq"

export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_DIAGNOSIS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_PATIENT_HEME_DIAGNOSIS
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


