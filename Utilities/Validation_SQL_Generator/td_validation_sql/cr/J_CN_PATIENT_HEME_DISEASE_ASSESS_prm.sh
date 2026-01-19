
export JOBNAME='J_CN_PATIENT_HEME_DISEASE_ASSESS'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_HEME_DISEASE_ASSESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM (
Sel iq.* from
(Sel 
Trim(HBSource) as Hashbite_SSK
From EDWCR_Staging.PATIENT_HEME_DISEASE_ASSESSMENT_STG PHD 
Left Outer Join EDWCR_Base_Views.Ref_Test_Type RT
On Trim(PHD.TestType)=Trim(RT.Test_Sub_Type_Desc)
and Trim(RT.Test_Type_Desc) ='Disease Assessment'
Left Outer Join EDWCR_Base_Views.Ref_Sample_Type RST
On Trim(PHD.SampleSourceType) = Trim(RST.Sample_Type_Name)
Left Outer Join EDWCR_Base_Views.Ref_Disease_Assess_Source RDA
On Trim(PHD.Source) = Trim(RDA.Disease_Assess_Source_Name)
Left Outer Join EDWCR_Base_Views.Ref_Facility RF
On Trim(PHD.FacilityName) = Trim(RF.Facility_Name)
Left Outer Join EDWCR_Base_Views.Ref_Status RS
On  Trim(PHD.DiseaseStatus)=Trim(RS.Status_Desc)
and Trim(RS.Status_Type_Desc) ='Disease'
Left Outer Join EDWCR_Base_Views.Ref_Status RSS
On  Trim(PHD.TreatementStatus)=Trim(RSS.Status_Desc)
and Trim(RSS.Status_Type_Desc) ='Treatment'
) iq  
Left Outer Join EDWCR_BASE_VIEWS.CN_PATIENT_HEME_DISEASE_ASSESS CPHD
on Trim(iq.Hashbite_SSK) = Trim(CPHD.Hashbite_SSK)
where Trim(CPHD.Hashbite_SSK) is null
) iqq"

export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_DISEASE_ASSESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $EDWCR_BASE_VIEWS.CN_PATIENT_HEME_DISEASE_ASSESS
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


