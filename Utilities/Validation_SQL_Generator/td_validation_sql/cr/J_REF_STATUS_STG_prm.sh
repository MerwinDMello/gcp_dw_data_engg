export JOBNAME='J_REF_STATUS_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_STATUS_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
Select distinct NavigationStatus as StatusDesc, 'Navigation' AS Status_Type_Desc from Navadhoc.dbo.PatientTumor (NOLOCK) where NavigationStatus is not null and NavigationStatus <> ''
Union 
Select distinct TaskState as StatusDesc, 'Task' AS Status_Type_Desc from Navadhoc.dbo.SurvivorShipCarePlan (NOLOCK) where TaskState is not null and TaskState <> ''
union
Select distinct TransplantCandidacyStatus as StatusDesc, 'Cellular Therapy' AS Status_Type_Desc from Navadhoc.dbo.PatientHemeTransplant (NOLOCK) where TransplantCandidacyStatus is not null and TransplantCandidacyStatus <> ''
Union 
Select distinct DiseaseStatus as StatusDesc, 'Disease' AS Status_Type_Desc from Navadhoc.dbo.PatientImaging (NOLOCK) where DiseaseStatus is not null and DiseaseStatus <> ''
Union 
Select distinct TreatementStatus as StatusDesc, 'Treatment' AS Status_Type_Desc from Navadhoc.dbo.PatientImaging (NOLOCK) where TreatementStatus is not null and TreatementStatus <> ''
union
Select distinct DiseaseStatus as StatusDesc, 'Disease' AS Status_Type_Desc from Navadhoc.dbo.PatientHemeDiagnosis (NOLOCK) where DiseaseStatus is not null and DiseaseStatus <> ''
Union 
Select distinct DiseaseStatus as StatusDesc, 'Disease' AS Status_Type_Desc from Navadhoc.dbo.PatientHemeDiseaseAssessment (NOLOCK) where DiseaseStatus is not null and DiseaseStatus <> ''
Union 
Select distinct TreatementStatus as StatusDesc, 'Treatment' AS Status_Type_Desc from Navadhoc.dbo.PatientHemeDiseaseAssessment (NOLOCK) where TreatementStatus is not null and TreatementStatus <> ''

) A"
export AC_ACT_SQL_STATEMENT="Select 'J_REF_STATUS_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_STATUS_STG"
