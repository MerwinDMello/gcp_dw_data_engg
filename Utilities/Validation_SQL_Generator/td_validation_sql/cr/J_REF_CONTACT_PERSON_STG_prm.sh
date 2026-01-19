export JOBNAME='J_REF_CONTACT_PERSON_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_REF_CONTACT_PERSON_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
 from (SELECT Distinct PersonofContact FROM Navadhoc.dbo.PatientCommunication (NOLOCK)
where PersonofContact is not null and ltrim(rtrim(PersonofContact)) <> '')
ab"

export AC_ACT_SQL_STATEMENT="Select 'J_REF_CONTACT_PERSON_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Contact_Person_stg"
