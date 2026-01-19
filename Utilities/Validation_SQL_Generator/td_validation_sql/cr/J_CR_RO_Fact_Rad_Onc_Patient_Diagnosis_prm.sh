
export JOBNAME='J_CR_RO_Fact_Rad_Onc_Patient_Diagnosis'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_RO_Fact_Rad_Onc_Patient_Diagnosis'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(select	
dp.Fact_Patient_SK,
rpp.Diagnosis_Code_SK,
rp.Patient_Sk ,
dp.Diagnosis_Status_Id,
dp.Cell_Category_Id,
dp.Cell_Grade_Id,
dp.Laterality_Id,
dp.Stage_Id,
dp.Stage_Status_Id,
dp.Recurrence_Id,
dp.Invasion_Id,
dp.Confirmed_Diagnosis_Id,
dp.Diagnosis_Type_Id,
rr.Site_SK ,
dp.Source_Fact_Patient_Diagnosis_Id,
dp.Diagnosis_Status_Date,
dp.Diagnosis_Text,
dp.Clinical_Text,
dp.Pathology_Comment_Text,
dp.Node_Num,
dp.Positive_Node_Num,
dp.Log_Id,
dp.Run_Id,   
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from
(
	SELECT
	Row_Number() Over (
	Order By Cast(DimSiteID as Int) ,Cast(FactPatientDiagnosisID as Int)) as Fact_Patient_SK,
	DimLookupID_DiagnosisStatus as Diagnosis_Status_Id,
	DimLookupID_CellCategory as Cell_Category_Id,
	DimLookupID_CellGrade as Cell_Grade_Id ,
	DimLookupID_Laterality as Laterality_Id,
	DimLookupID_Stage as Stage_Id,
	DimLookupID_StageStatus as Stage_Status_Id,
	DimLookupID_Recurrence as Recurrence_Id,
	DimLookupID_Invasive as Invasion_Id,
	DimLookupID_ConfirmedDx as Confirmed_Diagnosis_Id,
	DimLookupID_DiagnosisType as Diagnosis_Type_Id,
	FactPatientDiagnosisID as Source_Fact_Patient_Diagnosis_Id,
	CAST(CAST(DiagnosisStatusDate AS TIMESTAMP (6) ) AS DATE) as Diagnosis_Status_Date ,
	DiagnosisDescription as Diagnosis_Text,
	ClinicalDescription as Clinical_Text,
	PathologyComments as Pathology_Comment_Text,
	Nodes as Node_Num,
	NodesPositive as Positive_Node_Num,
	Cast(LogID as Int) as Log_Id,
	Cast(RunID as Int) as Run_Id,
	DimDiagnosisCoDeID,
	DimPatientID,
	DimSiteID
	FROM	edwcr_staging.stg_FactPatientDiagnosis) dp
inner join EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site rr 
	on dp.DimSiteID=rr.Source_Site_Id 

Left Outer Join EDWCR_BASE_VIEWS.Ref_Rad_Onc_diagnosis_Code rpp 
	on rpp.Source_Diagnosis_Code_Id = dp.DimDiagnosisCoDeID
	and rpp.Site_SK=rr.Site_SK

Left Outer Join EDWCR_BASE_VIEWS.Rad_Onc_Patient rp 
	on rp.Source_Patient_Id = dp.DimPatientID 
	and rp.Site_SK=rr.Site_SK)STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_RO_Fact_Rad_Onc_Patient_Diagnosis'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Fact_Rad_Onc_Patient_Diagnosis
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


