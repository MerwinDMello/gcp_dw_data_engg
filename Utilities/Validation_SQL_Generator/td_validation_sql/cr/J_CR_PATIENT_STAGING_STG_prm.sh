####. /etl/jfmd/EDWCR/ParmFiles/J_CR_REF_TUMOR_STG_prm.sh

export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_STAGING_STG'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_PATIENT_STAGING_STG' + ','+ CAST(SUM(TOT_COUNT) AS VARCHAR(20))+',' AS SOURCE_STRING FROM
(
SELECT COUNT(*) AS TOT_COUNT FROM
(
Select	
ext1.Patientid,	
ext1.Tumorid,	
'C' AS Cancer_Stage_Classification_Method_Code,	
'T' AS Cancer_Stage_Type_Code,	
ext1.Clin_T_TNM,	
ext1.AJCCStageGroupClin,
tum1.code,
tum1.sub,
tum1.group1
from mregistry.dbo.tumorext1b ext1	
LEFT JOIN
(
SELECT  distinct		
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupClin end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupClin_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1		
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupClin		
    AND SQ1.GroupID = L2371.GroupID		
AND L2371.SubCode=ClinGrpSubCode		
)tum1
ON tum1.tumorid1 =ext1.Tumorid
where Clin_T_TNM is not null


union	

Select	
ext2.Patientid,	
ext2.Tumorid,	
'C' AS Cancer_Stage_Classification_Method_Code,	
'N' AS Cancer_Stage_Type_Code,	
ext2.Clin_N_TNM,	
ext2.AJCCStageGroupClin,
tum2.code,
tum2.sub,
tum2.group1	
from mregistry.dbo.tumorext1b ext2	
LEFT JOIN
(
SELECT  distinct		
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupClin end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupClin_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1		
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupClin		
    AND SQ1.GroupID = L2371.GroupID		
AND L2371.SubCode=ClinGrpSubCode		
)tum2
ON tum2.tumorid1 =ext2.Tumorid
where Clin_N_TNM is not null

union	

Select	
ext3.Patientid,	
ext3.Tumorid,	
'C' AS Cancer_Stage_Classification_Method_Code,	
'M' AS Cancer_Stage_Type_Code,	
ext3.Clin_M_TNM,	
ext3.AJCCStageGroupClin,
tum3.code,
tum3.sub,
tum3.group1	
from mregistry.dbo.tumorext1b ext3	
LEFT JOIN
(
SELECT  distinct		
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupClin end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupClin_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1		
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupClin		
    AND SQ1.GroupID = L2371.GroupID		
AND L2371.SubCode=ClinGrpSubCode		
)tum3
ON tum3.tumorid1 =ext3.Tumorid
where Clin_M_TNM is not null

union	

Select	
ext4.Patientid,	
ext4.Tumorid,	
'C' AS Cancer_Stage_Classification_Method_Code,	
null AS Cancer_Stage_Type_Code,	
null as Clin_T_TNM,	
ext4.AJCCStageGroupClin,
tum4.code,
tum4.sub,
tum4.group1	
 from mregistry.dbo.tumorext1b ext4	
LEFT JOIN
(
SELECT  distinct		
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupClin end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupClin_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1		
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupClin		
    AND SQ1.GroupID = L2371.GroupID		
AND L2371.SubCode=ClinGrpSubCode		
)tum4
ON tum4.tumorid1 =ext4.Tumorid	
where Clin_M_TNM is  null and Clin_N_TNM is  null and Clin_T_TNM is  null	

union

Select	
ext5.Patientid,	
ext5.Tumorid,	
'P' AS Cancer_Stage_Classification_Method_Code,	
'T'  AS Cancer_Stage_Type_Code,	
ext5.Path_T_TNM,	
ext5.AJCCStageGroupPath,
tum5.code,
tum5.sub,
tum5.group1	
from mregistry.dbo.tumorext1b	ext5
LEFT JOIN (
SELECT  distinct
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupPath end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupPath_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupPath 		
AND L2371.SubCode= PathGrpSubCode		
    AND SQ1.GroupID = L2371.GroupID
)tum5
ON 
ext5.Tumorid= tum5.tumorid1	
where Path_T_TNM is not null	


union	

Select	
ext6.Patientid,	
ext6.Tumorid,	
'P' AS Cancer_Stage_Classification_Method_Code,	
'N' AS Cancer_Stage_Type_Code,	
ext6.Path_N_TNM,	
ext6.AJCCStageGroupPath,
tum6.code,
tum6.sub,
tum6.group1	
from mregistry.dbo.tumorext1b	ext6
LEFT JOIN (
SELECT  distinct
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupPath end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupPath_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupPath 		
AND L2371.SubCode= PathGrpSubCode		
    AND SQ1.GroupID = L2371.GroupID
)tum6
ON 
ext6.Tumorid= tum6.tumorid1	
where Path_N_TNM is not null
	
union	

Select	
ext7.Patientid,	
ext7.Tumorid,	
'P' AS Cancer_Stage_Classification_Method_Code,	
'M' AS Cancer_Stage_Type_Code,	
ext7.Path_M_TNM,	
ext7.AJCCStageGroupPath	,
tum7.code,
tum7.sub,
tum7.group1
from mregistry.dbo.tumorext1b	ext7

LEFT JOIN (
SELECT  distinct
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupPath end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupPath_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupPath 		
AND L2371.SubCode= PathGrpSubCode		
    AND SQ1.GroupID = L2371.GroupID
)tum7
ON 
ext7.Tumorid= tum7.tumorid1
where Path_M_TNM is not null	


union 

Select	
ext8.Patientid,	
ext8.Tumorid,	
'P' AS Cancer_Stage_Classification_Method_Code,	
null AS Cancer_Stage_Type_Code,	
null as Path_T_TNM,	
ext8.AJCCStageGroupPath,
tum8.code,
tum8.sub,
tum8.group1	
from mregistry.dbo.tumorext1b	ext8

LEFT JOIN (
SELECT  distinct
Case when L2371.Description is null or  L2371.Description= '' then '-99' else 		
TumorExt1b.AJCCStageGroupPath end as code,		
Case when L2371.Description is null or  L2371.Description= ''  then 'Unknown Description' else L2371.Description end as AJCCStageGroupPath_Desc		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else L2371.SubCode end as sub		
,Case when L2371.Description is null or  L2371.Description= ''  then -99 else SQ1.GroupID end as group1
,Tumor.tumorid as tumorid1
FROM MRegistry.dbo.Tumor 		
 JOIN MRegistry.dbo.TumorExt1b 		
  ON Tumor.tumorid = TumorExt1b.tumorid 		
Inner JOIN 		
(		
  SELECT		
   MAX (GROUPID) as GroupID		
  ,LookupID		
  ,t1.TumorID		
  FROM 		
  MRegistry.dbo.Tumor t1		
  JOIN MRegistry.dbo.TumorExt1b t2		
  ON t1.tumorid = t2.tumorid 		
  INNER JOIN MDictionary.dbo.LookupGroups t3		
    ON  t3.beginhisto <= left(t1.histology,4)		
    AND t3.endhisto >= left(t1.histology,4) 		
    AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 		
    AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END		
    AND t3.beginedition <= t2.tnm_Edition AND t3.endedition >= t2.tnm_Edition		
    AND LookupID = '4040'		
  GROUP BY		
   LookupID		
  ,t1.TumorID 		
)SQ1		
    ON Tumor.TumorID = SQ1.TumorID		
    --and Patient.patientid = SQ1.PatientID		
    --and Treatment.TreatmentID = SQ1.TreatmentID		
LEFT JOIN [MDictionary].dbo.LookupCodes as L2371 		
    ON L2371.LookupID = '4040'		
    AND L2371.Code = AJCCStageGroupPath 		
AND L2371.SubCode= PathGrpSubCode		
    AND SQ1.GroupID = L2371.GroupID
)tum8
ON 
ext8.Tumorid= tum8.tumorid1	
where Path_M_TNM is  null and Path_N_TNM is  null and Path_T_TNM is NULL 
	

)B
)X"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_STAGING_STG'||','|| CAST(SUM(TOT_COUNT) AS VARCHAR(20))||',' as SOURCE_STRING 
from 
(
SELECT COUNT(*) AS TOT_COUNT FROM EDWCR_Staging.CR_PATIENT_STAGING_SAMPLE_STAGE

)X"

