export JOBNAME='J_REF_SITE_LOCATION_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_SITE_LOCATION_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from
(
SELECT DISTINCT BiopsySite FROM	Navadhoc.dbo.PatientBiopsy (NOLOCK)
where BiopsySite is not null
UNION
SELECT DISTINCT ROLocation FROM Navadhoc.dbo.PatientradiationOncology (NOLOCK)
where ROLocation is not null
UNION
select DISTINCT TumorDiseaseSite from Navadhoc.dbo.PatientHemeRiskFactor (NOLOCK)
where TumorDiseaseSite IS NOT NULL
UNION
select DISTINCT OtherTumorDiseaseSite from Navadhoc.dbo.PatientHemeRiskFactor (NOLOCK)
where OtherTumorDiseaseSite IS NOT NULL
 )A"


export AC_ACT_SQL_STATEMENT="Select 'J_REF_SITE_LOCATION_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_SITE_LOCATION_STG"




