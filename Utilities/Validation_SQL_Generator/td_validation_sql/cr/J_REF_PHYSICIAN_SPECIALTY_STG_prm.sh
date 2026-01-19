export JOBNAME='J_REF_PHYSICIAN_SPECIALTY_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_PHYSICIAN_SPECIALTY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
SELECT Distinct BiopsyPhysicianType
FROM Navadhoc.dbo.PatientBiopsy (NOLOCK)
Where BiopsyPhysicianType is not NULL) A"
export AC_ACT_SQL_STATEMENT="Select 'J_REF_PHYSICIAN_SPECIALTY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Ref_Physician_Specialty_Stg"
