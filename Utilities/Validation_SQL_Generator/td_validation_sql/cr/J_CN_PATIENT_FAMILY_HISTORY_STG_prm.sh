export JOBNAME='J_CN_PATIENT_FAMILY_HISTORY_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_FAMILY_HISTORY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select
PatientHistoryFactID,
PatientDimID,
Coid,
61 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(FamilyHistory as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
patienthistory Where LTRIM(RTRIM(FamilyHistory)) is not null and LTRIM(RTRIM(FamilyHistory))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
62 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(Lineage as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(Lineage)) is not null and LTRIM(RTRIM(Lineage))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
63 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(Age as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(Age)) is not null and LTRIM(RTRIM(Age))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
64 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(RelationToPatient as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(RelationToPatient)) is not null and LTRIM(RTRIM(RelationToPatient))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
65 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(TumorSite as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(TumorSite)) is not null and LTRIM(RTRIM(TumorSite))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
66 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(AshkenaziJewish as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(AshkenaziJewish)) is not null and LTRIM(RTRIM(AshkenaziJewish))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
67 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(EverSmoked as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(EverSmoked)) is not null and LTRIM(RTRIM(EverSmoked))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
68 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(SmokingCessationOffered as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(SmokingCessationOffered)) is not null and LTRIM(RTRIM(SmokingCessationOffered))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
69 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(QuitSmoking as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(QuitSmoking)) is not null and LTRIM(RTRIM(QuitSmoking))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
70 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(DateQuitSmoking as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(DateQuitSmoking)) is not null and LTRIM(RTRIM(DateQuitSmoking))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
71 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(PackPerDay as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(PackPerDay)) is not null and LTRIM(RTRIM(PackPerDay))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
72 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(Years as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(Years)) is not null and LTRIM(RTRIM(Years))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
73 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(PackYearHx as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(PackYearHx)) is not null and LTRIM(RTRIM(PackYearHx))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
74 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(TakingEstrogen as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(TakingEstrogen)) is not null and LTRIM(RTRIM(TakingEstrogen))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
75 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(TakingProgesterone as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(TakingProgesterone)) is not null and LTRIM(RTRIM(TakingProgesterone))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
76 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(ReceptorsER as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(ReceptorsER)) is not null and LTRIM(RTRIM(ReceptorsER))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
77 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(ReceptorsPR as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(ReceptorsPR)) is not null and LTRIM(RTRIM(ReceptorsPR))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
78 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(AdvisedToQuitER as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(AdvisedToQuitER)) is not null and LTRIM(RTRIM(AdvisedToQuitER))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
79 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(AdvisedToQuitPR as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(AdvisedToQuitPR)) is not null and LTRIM(RTRIM(AdvisedToQuitPR))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
80 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(OnOrAboutDateER as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(OnOrAboutDateER)) is not null and LTRIM(RTRIM(OnOrAboutDateER))<>''

UNION

Select
PatientHistoryFactID,
PatientDimID,
Coid,
81 as Family_History_Query_Id,
LTRIM(RTRIM(CAST(OnOrAboutDatePR as varchar(1000)))) as Family_History_Value_Text,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource
from
navadhoc.dbo.patienthistory Where LTRIM(RTRIM(OnOrAboutDatePR)) is not null and LTRIM(RTRIM(OnOrAboutDatePR))<>''
) SRC"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_FAMILY_HISTORY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Family_History_STG"
