####. /etl/jfmd/EDWCR/ParmFiles/J_CR_REF_TUMOR_STG_prm.sh

export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_TREATMENT_STG'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_PATIENT_TREATMENT_STG' + ','+ CAST(SUM(TOT_COUNT) AS VARCHAR(20))+',' AS SOURCE_STRING FROM
(
SELECT COUNT(*) AS TOT_COUNT FROM
(
SELECT 
TreatmentID,
TumorId,
TreatmentHospID,
RxCode,
OtherCode,
SurgMarg,
RxType,
DtProtocol,
DtRxStart,
cast(ProtTitle as varchar(1000)) as ProtTitle,
 cast(RxTxt as varchar(4000)) as RxTxt,
Rx_MD1
FROM MRegistry.dbo.Treatment
)a

UNION ALL

SELECT COUNT(*) AS TOT_COUNT  FROM
(
SELECT 
Treatment.TreatmentID,
Treatment.RxCode
, L2371.Description
,L2371.GroupID
FROM MRegistry.dbo.Patient 
INNER JOIN MRegistry.dbo.TumorExt1a on TumorExt1a.PatientID = Patient.PatientID
LEFT OUTER JOIN MRegistry.dbo.Treatment on Treatment.TumorID = TumorExt1a.TumorID
LEFT OUTER JOIN [MDictionary].dbo.LookupCodes as L2371 
    ON L2371.LookupID = 4043
    AND L2371.Code = Treatment.RxCode
    AND L2371.SubCode = Treatment.rxsubcode
INNER JOIN 
(
    SELECT
     PatientID
    ,t1.TumorID
    ,t2.TreatmentID
    ,MAX (t3.groupID) as GroupID
    FROM
        MRegistry.dbo.Tumor t1
    LEFT JOIN    
        MRegistry.dbo.Treatment t2
        ON t1.TumorID = t2.TumorID
    INNER JOIN    
        MDictionary.dbo.LookupGroups t3
        ON  t3.beginhisto <= left(t1.histology,4)
        AND t3.endhisto >= left(t1.histology,4) 
        AND t3.beginprimarysite <= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END 
        AND t3.endprimarysite >= CASE RIGHT(t1.primarysite, 3) WHEN '***' THEN '000' ELSE RIGHT(t1.primarysite, 3) END
        AND t3.beginrxtype <= CASE t2.rxtype WHEN 'D' THEN '0' WHEN 'P' THEN '1' WHEN 'S' THEN '2' WHEN 'R' THEN '3' WHEN 'C' THEN '4' WHEN 'H' THEN '5' WHEN 'I' THEN '6' WHEN 'T' THEN '7' WHEN 'O' THEN '8' END 
        AND t3.endrxtype >= CASE t2.rxtype WHEN 'D' THEN '0' WHEN 'P' THEN '1' WHEN 'S' THEN '2' WHEN 'R' THEN '3' WHEN 'C' THEN '4' WHEN 'H' THEN '5' WHEN 'I' THEN '6' WHEN 'T' THEN '7' WHEN 'O' THEN '8' END 
        AND t3.LookupID = 4043
    GROUP BY 
     PatientID
    ,t1.TumorID 
    ,t2.TreatmentID 
)SQ1
    ON SQ1.GroupID = L2371.GroupID
    and Treatment.TreatmentID = SQ1.TreatmentID		
)B
)X"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_TREATMENT_STG'||','|| CAST(SUM(TOT_COUNT) AS VARCHAR(20))||',' as SOURCE_STRING 
from 
(
SELECT COUNT(*) AS TOT_COUNT FROM EDWCR_Staging.CR_PATIENT_TREATMENT_stg
UNION ALL
SELECT COUNT(*) AS TOT_COUNT FROM EDWCR_Staging.CR_PATIENT_TREATMENT_STG1
)X"

