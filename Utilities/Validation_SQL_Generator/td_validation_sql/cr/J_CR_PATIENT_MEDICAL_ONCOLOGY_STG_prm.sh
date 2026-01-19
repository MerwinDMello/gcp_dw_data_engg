export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_MEDICAL_ONCOLOGY_STG'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_PATIENT_MEDICAL_ONCOLOGY_STG' + ','+ CAST(COUNT(*) AS VARCHAR(20))+',' AS SOURCE_STRING FROM
(
Select distinct
d.CycleID,
c.TreatmentID,
d.DrugRoute,
d.DoseUnits,
d.DrugId,
d.DaysGiven,
d.DailyDose,
d.DrugHospID,
d.NSC,
c.DtCycleStrt,
d.CER_DrugEndDate,
c.CycleNum
from mRegistry.dbo.Drug d

Left Join mRegistry.dbo.Cycle c
on d.CycleId = c.CycleId ) a"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_MEDICAL_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_STAGING.CR_Patient_Medical_Oncology_STG"
