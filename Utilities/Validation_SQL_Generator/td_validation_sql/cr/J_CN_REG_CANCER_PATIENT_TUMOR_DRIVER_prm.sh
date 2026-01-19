export JOBNAME='J_CN_REG_CANCER_PATIENT_TUMOR_DRIVER'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_REG_CANCER_PATIENT_TUMOR_DRIVER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from
(
sel 
ROW_NUMBER() OVER( ORDER BY Cancer_Patient_Driver_SK asc, T_SK asc , CR_Patient_ID asc , CN_Patient_Id asc , CP_Patient_Id asc , CR_Tumor_Primary_Site_Id asc , CN_Tumor_Type_Id asc , CP_ICD_Oncology_Code  ASC) AS Cancer_Patient_Tumor_Driver_SK,
a.*,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time

from
(
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,
T.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0)) AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code,
CPD.SOURCE_SYSTEM_CODE

FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
INNER JOIN                                              
(sel * from EDWCR.CANCER_TUMOR_DRIVER                                             
QUALIFY ROW_NUMBER() OVER(PARTITION BY CR_TUMOR_PRIMARY_SITE_ID ORDER BY CANCER_TUMOR_DRIVER_SK ASC)=1) t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
WHERE CPD.CN_PATIENT_ID IS NULL AND CPD.CP_PATIENT_ID IS NULL                                            
                                            
UNION                                            
/* CP only records */                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T2.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CAST(NULL AS integer) AS CR_PATIENT_ID,CAST(NULL AS INTEGER) AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
cast(CPNT.PATIENT_DW_ID as decimal(18,0)) AS CP_PATIENT_ID,cast(CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE as char(5)) AS CP_ICD_Oncology_Code    ,            
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
CPNT.Submitted_Primary_ICD_Oncology_CODE=T2.CP_ICD_Oncology_Code                                            
WHERE                                             
CPD.CR_PATIENT_ID IS NULL AND CPD.CN_PATIENT_ID IS NULL                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                             
ORDER BY T2.CANCER_TUMOR_DRIVER_SK ASC)=1            

UNION                                            
/* CN only records */                


SELECT   CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T1.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code        ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
WHERE                                             
CPD.CR_PATIENT_ID IS NULL AND CPD.CP_PATIENT_ID IS NULL                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* CR and CP only */                                            
                                            
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,T.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CPNT.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code    ,    
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
INNER JOIN                                              
EDWCR.CANCER_TUMOR_DRIVER T                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NULL                                            
/*and                                             
CANCER_PATIENT_DRIVER_SK=2246473*/                                            
AND T.CANCER_TUMOR_DRIVER_SK=T2.CANCER_TUMOR_DRIVER_SK                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,cpt.TUMOR_PRIMARY_SITE_ID ORDER BY T.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* Records for Extra CP when CR and CP not null and CN null */                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T2.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CPNT.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code            ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NULL /*AND                                             
CANCER_PATIENT_DRIVER_SK=2246473*/                                            
and (patient_dw_id,cpnt.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE) not in (                                            
SELECT  DISTINCT cpnt.patient_dw_id,cpnt.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
INNER JOIN                                              
EDWCR.CANCER_TUMOR_DRIVER T                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NULL                                            
/*and                                             
CANCER_PATIENT_DRIVER_SK=2246473*/                                            
AND T.CANCER_TUMOR_DRIVER_SK=T2.CANCER_TUMOR_DRIVER_SK                                            
                                            
)                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE ORDER BY T2.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* Records for Extra CR when CR and CP not null and CN null */                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code        ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
INNER JOIN                                              
EDWCR.CANCER_TUMOR_DRIVER T                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NULL /*AND                                              
CANCER_PATIENT_DRIVER_SK=2246473*/                                            
and (CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID) not in (                                            
SELECT  DISTINCT CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
INNER JOIN                                              
EDWCR.CANCER_TUMOR_DRIVER T                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NULL                                            
/*and                                             
CANCER_PATIENT_DRIVER_SK=2246473*/                                            
AND T.CANCER_TUMOR_DRIVER_SK=T2.CANCER_TUMOR_DRIVER_SK                                            
                                            
)                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID ORDER BY T.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* CR and CN only */                                            
                                            
/* Records when CR and CN not null and CP null  and tumor matches*/                                            
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,T.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code        ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
INNER JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
where  CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  NULL and CPD.CN_PATIENT_ID IS NOT NULL                                            
AND T.CANCER_TUMOR_DRIVER_SK=T1.CANCER_TUMOR_DRIVER_SK                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID, CPNT.TUMOR_TYPE_ID                                             
ORDER BY T.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
/*and cancer_patient_driver_sk=1872902*/                                            
union                                            
/* Records for Extra CN when CR and CN not null and CP null */                                            
                                            
SELECT   CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T1.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
   WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  NULL and CPD.CN_PATIENT_ID IS not NULL /*AND                                              
CANCER_PATIENT_DRIVER_SK=294059*/                                            
and (nav_patient_id,cpnt.tumor_type_id) not in (                                            
                                            
SELECT  DISTINCT cpnt.nav_patient_id,cpnt.tumor_type_id AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
INNER JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
where  CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  NULL and CPD.CN_PATIENT_ID IS NOT NULL                                            
AND T.CANCER_TUMOR_DRIVER_SK=T1.CANCER_TUMOR_DRIVER_SK                                            
/*and cancer_patient_driver_sk=294059*/)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* Records for Extra CR when CR and CN not null and CP null */                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code        ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
INNER JOIN                                              
EDWCR.CANCER_TUMOR_DRIVER T                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  NULL and CPD.CN_PATIENT_ID IS not NULL /*AND                                              
CANCER_PATIENT_DRIVER_SK=294059*/                                            
and (CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID) not in (                                            
SELECT  DISTINCT                                             
CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
INNER JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
where  CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  NULL and CPD.CN_PATIENT_ID IS NOT NULL                                            
AND T.CANCER_TUMOR_DRIVER_SK=T1.CANCER_TUMOR_DRIVER_SK                                            
/*and cancer_patient_driver_sk=294059*/                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID ORDER BY T.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
-- CP and CN not null and CR Null ---                                            
 union                                            
                                            
/* Both CN and CP have data  and no data in CR*/                                            
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,T1.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CPNT2.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code                        ,                    
CPD.SOURCE_SYSTEM_CODE                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
inner join                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS  NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS NOT NULL                                             
and T1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID,                                            
cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
/* Extra data in CN where CN and CP are not null and CR is null */                                            
union                                            
SELECT   CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T1.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
   WHERE CPD.CR_PATIENT_ID IS  NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS not NULL                                             
and (nav_patient_id,cpnt.tumor_type_id) not in (                                            
SELECT  DISTINCT                                             
cpnt.nav_patient_id,cpnt.tumor_type_id                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
inner join                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS  NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS NOT NULL                                             
and T1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                 
union                                            
/* Extra data in CP where CN and CP are not null and CR is null */                                            
                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T2.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CPNT2.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS  NULL AND CPD.CP_PATIENT_ID IS not  NULL and CPD.CN_PATIENT_ID IS not NULL                                            
and (CPNT2.patient_dw_id,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE)                                             
not in (                                            
SELECT  DISTINCT                                             
CPNT2.patient_dw_id,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
inner join                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
INNER JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS  NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS NOT NULL                                             
and T1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE ORDER BY T2.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* all 3 match tumor types*/                                            
                                            
                                            
                                            
SELECT   CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T1.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CPNT2.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPNT2.Submitted_Primary_ICD_Oncology_CODE,CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID,CPNT.TUMOR_TYPE_ID ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
                                            
/*  Matching CR and CN without CP match */                                            
                                            
SELECT   CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T1.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code                    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and                                            
CANCER_PATIENT_DRIVER_SK=104575*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK) and  (CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID ) not in                                            
(                                            
SELECT   CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID                                             
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK )                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID, CPT.TUMOR_PRIMARY_SITE_ID ORDER BY T1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
                                            
union                                            
                                            
/* Matching CP and CN data with CR not macthing */                                            
                                            
SELECT  DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,t1.CANCER_TUMOR_DRIVER_SK  AS T1_SK                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CPNT2.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code    ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK) and  (cpnt2.patient_dw_id,cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE ) not in                                            
(                                            
SELECT   cpnt2.patient_dw_id,cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK )                                            
                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID ORDER BY t1.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
union                                            
/* CR and CP data with not mataching CN data */                                            
                                            
SELECT  DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CPNT2.PATIENT_DW_ID AS CP_PATIENT_ID,CPNT2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code        ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t2.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK) and  (CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID ) not in                                            
(                                            
SELECT   CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID                                             
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK )                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID ORDER BY t.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
/* All remaining CR records*/                                            
                                            
union                                            
                                            
                                            
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,T.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
,CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID , CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code            ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
INNER JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
where  CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and (CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID)                                             
not in (  /* exclude CR data if CR and CN match */                                            
SELECT  DISTINCT CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)           

union                                            
/* exclude CR data if all 3  match */                                            
SELECT  DISTINCT CPT.CR_PATIENT_ID,
CPT.TUMOR_PRIMARY_SITE_ID AS T1_SK    
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK    


union                                            
/* exclude CR data if CR and CP match*/                                            
SELECT  DISTINCT CPT.CR_PATIENT_ID,CPT.TUMOR_PRIMARY_SITE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t2.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPT.TUMOR_PRIMARY_SITE_ID ORDER BY CANCER_TUMOR_DRIVER_SK ASC)=1                                            
                                            
/* All remaining CN data */                                            
                                            
union                                            
SELECT  DISTINCT                                             
CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK,T1.CANCER_TUMOR_DRIVER_SK AS T_SK                                            
                                            
,NULL AS CR_PATIENT_ID,NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CPNT.NAV_PATIENT_ID AS CN_PATIENT_ID , CPNT.TUMOR_TYPE_ID AS CN_TUMOR_TYPE_ID ,                                            
CAST(NULL AS DECIMAL(18,0)) AS CP_PATIENT_ID,CAST(NULL AS char(5)) AS CP_ICD_Oncology_Code         ,
CPD.SOURCE_SYSTEM_CODE
                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
where  CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS  not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and ( CPNT.NAV_PATIENT_ID,CPNT.TUMOR_TYPE_ID )                                             
not in (/* exclude CN data if CR and CN match*/                                            
SELECT  DISTINCT CPNT.NAV_PATIENT_ID,CPNT.TUMOR_TYPE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)    



union                                            
/* exclude CN data if all 3 match*/                                            
SELECT  DISTINCT CPNT.NAV_PATIENT_ID,
CPNT.TUMOR_TYPE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK           

union                                            
/* exclude CN data if CN and CP match*/                                            
SELECT  DISTINCT CPNT.NAV_PATIENT_ID,CPNT.TUMOR_TYPE_ID AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.TUMOR_TYPE_ID ORDER BY CANCER_TUMOR_DRIVER_SK ASC)=1              


union                                            
                                            
/* All remaining CP data*/                                            
                                            
SELECT   DISTINCT CPD.COID,
CPD.COMPANY_CODE,
CPD.CANCER_PATIENT_DRIVER_SK                                            
,T2.CANCER_TUMOR_DRIVER_SK AS T1_SK                                            
,NULL AS CR_PATIENT_ID,
NULL AS CR_TUMOR_PRIMARY_SITE_ID,                                            
CAST(NULL AS DECIMAL(18,0))AS CN_PATIENT_ID ,
CAST(NULL AS INTEGER) AS CN_TUMOR_TYPE_ID ,                                            
CPNT.PATIENT_DW_ID AS CP_PATIENT_ID,
CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS CP_ICD_Oncology_Code      ,
CPD.SOURCE_SYSTEM_CODE
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT                                            
ON CPD.CP_PATIENT_ID = CPNT.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and (patient_dw_id,cpnt.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE) not                                             
in (/* exclude CP data if CR and CP match*/                                            
SELECT  DISTINCT cpnt2.patient_dw_id,cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t2.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t1.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)                                            
union                                            
/* exclude CPdata if all 3 match*/                                            
SELECT  DISTINCT cpnt2.patient_dw_id,cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t.CANCER_TUMOR_DRIVER_SK and t.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK       

union                             

/* exclude CP data if CN and CP match*/                                            
SELECT  DISTINCT cpnt2.patient_dw_id,cpnt2.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE AS T1_SK                                            
FROM  EDWCR.CANCER_PATIENT_DRIVER CPD                                            
INNER JOIN                                            
EDWCR_BASE_VIEWS.CR_PATIENT_TUMOR     CPT                                                                      
ON CPD.CR_PATIENT_ID=CPT.CR_PATIENT_ID                                            
                                            
                                            
inner JOIN  EDWCR.CANCER_TUMOR_DRIVER t                                            
ON   CPT.TUMOR_PRIMARY_SITE_ID = T.CR_TUMOR_PRIMARY_SITE_ID                                             
inner JOIN                                            
EDWCR_BASE_VIEWS.CN_PATIENT_TUMOR CPNT                                            
ON CPD.CN_PATIENT_ID = CPNT.NAV_PATIENT_ID                                            
inner JOIN EDWCR_BASE_VIEWS.REF_TUMOR_TYPE RTT                                            
ON CPNT.TUMOR_TYPE_ID=RTT.TUMOR_TYPE_ID                                            
                                            
inner JOIN EDWCR.CANCER_TUMOR_DRIVER T1                                            
ON                                               
(CASE WHEN                                             
RTT.TUMOR_TYPE_GROUP_NAME='GENERAL' THEN T1.CN_GENERAL_TUMOR_TYPE_ID                                            
WHEN RTT.TUMOR_TYPE_GROUP_NAME='NAVQ' THEN T1.CN_NAVQUE_TUMOR_TYPE_ID                                            
ELSE T1.CN_TUMOR_TYPE_ID                                            
END) = CPNT.TUMOR_TYPE_ID                                             
                                            
                                            
inner JOIN                                            
(                                            
sel DISTINCT patient_dw_id,SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE                                            
FROM EDWCR_BASE_VIEWS.cancer_patient_id_output) CPNT2                                            
ON CPD.CP_PATIENT_ID = CPNT2.patient_dw_id                                            
                                            
INNER JOIN EDWCR.CANCER_TUMOR_DRIVER T2                                            
ON                                               
T2.CP_ICD_Oncology_Code= CPNT2.Submitted_Primary_ICD_Oncology_CODE                                            
WHERE   CPD.CR_PATIENT_ID IS not NULL AND CPD.CP_PATIENT_ID IS not NULL and CPD.CN_PATIENT_ID IS NOT NULL /*and cancer_patient_driver_sk=107567*/                                            
and t1.CANCER_TUMOR_DRIVER_SK=t2.CANCER_TUMOR_DRIVER_SK and                                             
(                                            
t.CANCER_TUMOR_DRIVER_SK<>t1.CANCER_TUMOR_DRIVER_SK                                            
OR t.CANCER_TUMOR_DRIVER_SK<>t2.CANCER_TUMOR_DRIVER_SK)                                            
)                                            
QUALIFY ROW_NUMBER() OVER(PARTITION BY CPD.CANCER_PATIENT_DRIVER_SK,CPNT.SUBMITTED_PRIMARY_ICD_ONCOLOGY_CODE ORDER BY t2.CANCER_TUMOR_DRIVER_SK ASC)=1                                            
)a
)A"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_REG_CANCER_PATIENT_TUMOR_DRIVER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CANCER_PATIENT_TUMOR_DRIVER"
