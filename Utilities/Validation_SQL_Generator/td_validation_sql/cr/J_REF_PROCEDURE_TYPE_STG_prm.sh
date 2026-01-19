export JOBNAME='J_REF_PROCEDURE_TYPE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_PROCEDURE_TYPE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING From
(
SELECT Distinct ltrim(rtrim(ProcedureType)) AS ProcedureType,
case 
when ltrim(rtrim(LinePlacementType)) <> ''  and ltrim(rtrim(LinePlacementType)) is not NULL then ltrim(rtrim(LinePlacementType)) 
when ltrim(rtrim(OtherProcedureType)) <> ''  and ltrim(rtrim(OtherProcedureType)) is not NULL then ltrim(rtrim(OtherProcedureType)) 
when ltrim(rtrim(OtherSurgeryType)) <> ''  and ltrim(rtrim(OtherSurgeryType)) is not NULL then ltrim(rtrim(OtherSurgeryType))
else null end AS Procedure_Sub_Type_Desc 
FROM Navadhoc.dbo.PatientProcedure (NOLOCK)
Where ProcedureType is not null and ltrim(rtrim(ProcedureType)) <> ''
) A"
export AC_ACT_SQL_STATEMENT="Select 'J_REF_PROCEDURE_TYPE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.PROCEDURE_TYPE_STG"
