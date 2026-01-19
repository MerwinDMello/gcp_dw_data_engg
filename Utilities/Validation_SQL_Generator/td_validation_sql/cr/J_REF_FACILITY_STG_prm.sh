export JOBNAME='J_REF_FACILITY_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_FACILITY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from ( 

Select Distinct rtrim(ltrim(A.FacilityName)) As FAC
From (
SELECT
ROFacility as FacilityName
FROM Navadhoc.dbo.PatientRadiationOncology (NOLOCK)

union all

SELECT
TumorReferralSource as FacilityName
FROM Navadhoc.dbo.PatientTumor  (NOLOCK)

UNION all

SELECT
ImageCenter as FacilityName
FROM Navadhoc.dbo.PatientImaging  (NOLOCK)

union all

SELECT
BiopsyFacility as FacilityName
FROM Navadhoc.dbo.PatientBiopsy  (NOLOCK)

union all

SELECT
MOFacility as FacilityName
FROM Navadhoc.dbo.PatientMedicalOncology  (NOLOCK)

union all

SELECT
EndTreatmentLocation as FacilityName
FROM Navadhoc.dbo.PatientTumor  (NOLOCK)

union all

SELECT
SurgeryFacility as FacilityName
From Navadhoc.dbo.PatientSurgery  (NOLOCK)

union all

SELECT
FacilityName as FacilityName
From Navadhoc.dbo.PatientHemeDiseaseAssessment  (NOLOCK)

) as A

)b"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_FACILITY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_FACILITY_STG"
