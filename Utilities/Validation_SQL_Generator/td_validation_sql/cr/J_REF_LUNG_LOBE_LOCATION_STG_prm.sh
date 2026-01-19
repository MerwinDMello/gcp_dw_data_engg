export JOBNAME='J_REF_LUNG_LOBE_LOCATION_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_LUNG_LOBE_LOCATION_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from ( Select distinct  ROLobes from Navadhoc.dbo.PatientRadiationOncology (NOLOCK)
 where ROLobes is not null )a"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_LUNG_LOBE_LOCATION_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_LUNG_LOBE_LOCATION_STG"
