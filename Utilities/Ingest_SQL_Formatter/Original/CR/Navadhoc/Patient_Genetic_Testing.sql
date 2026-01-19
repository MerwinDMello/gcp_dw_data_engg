Select 
GeneticsTestingFactID,
CoreRecordID,
PatientDimID,
TumorTypeDimId,
DiagnosisResultId,
DiagnosisDimID,
FacilityDimId,
Coid,
NavigatorDimId,
GeneticsDate,
REPLACE(REPLACE(LTRIM(RTRIM(GeneticsTestType)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as geneticstesttype,
REPLACE(REPLACE(LTRIM(RTRIM(GeneticsSpecialist)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as geneticsspecialist,
REPLACE(REPLACE(LTRIM(RTRIM(GeneticsBRCAType)), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as geneticsbrcatype,
REPLACE(REPLACE(LTRIM(RTRIM(cast(GeneticsComments as varchar(1000)))), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as GeneticsComments,
REPLACE(REPLACE(LTRIM(RTRIM(concat('0x',CONVERT(varchar(50),HBSource,2)))), NCHAR(8211), NCHAR(32)), NCHAR(8217), NCHAR(32)) as HBSource, 
'v_currtimestamp' as dw_last_update_date_time
from 
navadhoc.dbo.PatientGeneticTesting (NOLOCK);