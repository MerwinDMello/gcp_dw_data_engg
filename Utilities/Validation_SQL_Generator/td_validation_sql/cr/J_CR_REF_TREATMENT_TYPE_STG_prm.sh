export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_REF_TREATMENT_TYPE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_SITE_LOCATION_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from
(
SELECT 
distinct
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
    --and Patient.patientid = SQ1.PatientID
    and Treatment.TreatmentID = SQ1.TreatmentID
 )A"


export AC_ACT_SQL_STATEMENT="Select 'J_CR_REF_TREATMENT_TYPE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Ref_CR_Treatment_Type_Stg"




