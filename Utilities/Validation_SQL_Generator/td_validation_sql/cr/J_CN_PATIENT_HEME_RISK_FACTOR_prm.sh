export JOBNAME='J_CN_PATIENT_HEME_RISK_FACTOR'

export AC_EXP_SQL_STATEMENT="
SELECT	'J_CN_PATIENT_HEME_RISK_FACTOR'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT
STG.PatientHemeDiagnosisFactID as CN_Patient_Heme_Diagnosis_SID
,STG.PatientDimID as Nav_Patient_Id
,STG.TumorTypeDimID as Tumor_Type_Id
,STG.DiagnosisResultID as Diagnosis_Result_Id
,STG.DiagnosisDimID as Nav_Diagnosis_Id
,STG.COID as Coid
,'H' as Company_Code
,STG.NavigatorDimID as Navigator_Id
,trim(STG.RiskFactor) as Risk_Factor_Text
,trim(STG.OtherRiskFactor) as Other_Risk_Factor_Text
,PREV.site_location_id as Previous_Tumor_Site_Id
,OTH_PREV.site_location_id as Other_Previous_Tumor_Site_Id
,STG.Hashbite_SSK
,'N'AS Source_System_Code
,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time 
FROM EDWCR_STAGING.PATIENT_HEME_RISK_FACTOR_STG STG

LEFT JOIN EDWCR_BASE_VIEWS.Ref_Site_Location OTH_PREV
ON STG.OtherTumorDiseaseSite = OTH_PREV.Site_Location_Desc

LEFT JOIN EDWCR_BASE_VIEWS.Ref_Site_Location PREV
ON STG.TumorDiseaseSite = PREV.Site_Location_Desc

LEFT JOIN EDWCR.CN_PATIENT_HEME_RISK_FACTOR TGT
ON STG.Hashbite_SSK = TGT.Hashbite_SSK

WHERE TGT.Hashbite_SSK IS NULL AND TRIM(Risk_Factor_Text)<>Risk_Factor_Text

) A;"
 
export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_RISK_FACTOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.cn_patient_heme_risk_factor WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_HEME_RISK_FACTOR');"

