export JOBNAME='J_REF_SIDE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_SIDE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
SELECT SurgerySide AS Side_Desc FROM Navadhoc.dbo.PatientSurgery
UNION  
SELECT ImageArea AS Side_Desc FROM Navadhoc.dbo.PatientImaging
UNION 
SELECT ReconSurgerySide AS Side_Desc FROM Navadhoc.dbo.PatientSurgeryReconstructive 
UNION 
SELECT DiagnosisSide AS Side_Desc FROM Navadhoc.dbo.PatientDiagnosis) A"




export AC_ACT_SQL_STATEMENT="Select 'J_REF_SIDE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_SIDE_STG"
