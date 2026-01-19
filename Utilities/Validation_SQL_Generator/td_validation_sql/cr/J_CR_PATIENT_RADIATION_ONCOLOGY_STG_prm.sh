export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_RADIATION_ONCOLOGY_STG'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_PATIENT_RADIATION_ONCOLOGY_STG' + ','+ CAST(COUNT(*) AS VARCHAR(20))+',' AS SOURCE_STRING FROM
(
select 
RadiationId
,TreatmentId
,RT_Modality
,RadiationHospID
,DtRt_Start
,DTRt_Stop
from MRegistry.dbo.Radiation) a"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_RADIATION_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
${NCR_STG_SCHEMA}.CR_Patient_Radiation_Oncology_STG"
