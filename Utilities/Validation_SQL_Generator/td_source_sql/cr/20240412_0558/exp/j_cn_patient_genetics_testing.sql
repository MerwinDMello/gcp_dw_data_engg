Select 'J_CN_PATIENT_GENETICS_TESTING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
GeneticsTestingFactID,
CoreRecordID,
PatientDimID,
TumorTypeDimId,
DiagnosisResultId,
DiagnosisDimID,
Coid,
'H' as Company_Code,
NavigatorDimId,
GeneticsDate,
GeneticsTestType,
GeneticsSpecialist,
Breast_Cancer_Type_ID,
GeneticsComments,
HBSource,
'N' Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
edwcr_staging.CN_Patient_Genetics_Testing_STG PGT
left outer join edwcr.Ref_Breast_Cancer_Type BRCA
on 
 PGT.GeneticsBRCAType=BRCA. Breast_Cancer_Type_Desc
 where HBSource not in ( Select Hashbite_SSK from edwcr.CN_Patient_Genetics_Testing )
) SRC