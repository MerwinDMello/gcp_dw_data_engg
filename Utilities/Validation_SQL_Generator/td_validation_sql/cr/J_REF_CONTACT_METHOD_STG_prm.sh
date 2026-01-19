export JOBNAME='J_REF_CONTACT_METHOD_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_CONTACT_METHOD_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT Distinct TaskMeansofContact FROM Navadhoc.dbo.SurvivorShipCarePlan (NOLOCK)
where TaskMeansofContact is not null and ltrim(rtrim(TaskMeansofContact)) <> ''
UNION
Select Distinct MeansOfContact  from Navadhoc.dbo.PatientCommunication (NOLOCK))
a"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_CONTACT_METHOD_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_CONTACT_METHOD_STG"
