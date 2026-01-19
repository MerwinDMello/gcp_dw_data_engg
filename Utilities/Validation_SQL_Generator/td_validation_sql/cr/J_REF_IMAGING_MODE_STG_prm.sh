export JOBNAME='J_REF_IMAGING_MODE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_IMAGING_MODE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT
Distinct ImageMode
FROM Navadhoc.dbo.PatientImaging (NOLOCK)
where ImageMode is not NULL and ImageMode <> '')a"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_IMAGING_MODE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_IMAGING_MODE_STG"
