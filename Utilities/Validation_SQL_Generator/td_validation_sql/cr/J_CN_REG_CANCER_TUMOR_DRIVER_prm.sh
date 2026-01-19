export JOBNAME='J_CN_REG_CANCER_TUMOR_DRIVER'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_REG_CANCER_TUMOR_DRIVER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from
(
sel row_number() over (order by CR_Tumor_Primary_Site_Id,CN_Tumor_Type_Id,CN_General_Tumor_Type_Id,CN_Navque_Tumor_Type_Id,CP_ICD_Oncology_Code asc) as Cancer_Tumor_Driver_SK
,a.* from 
    (    SELECT DISTINCT 

        COALESCE (t3.ICD_Oncology_Code,'-99') AS CP_ICD_Oncology_Code
        ,COALESCE(t3.ICD_Oncology_Site_Desc,'Unknown Description') AS  CP_ICD_Oncology_Site_Desc
        ,COALESCE(COALESCE(t7.Tumor_Group,t3.ICD_Oncology_Site_Desc),'Unknown Description') AS CP_ICD_Oncology_Group_Name
        ,t1.Master_Lookup_SID AS CR_Tumor_Primary_Site_Id
        ,t4.Tumor_Type_ID AS CN_Tumor_Type_Id
        ,t5.Tumor_Type_ID AS CN_General_Tumor_Type_Id
        ,t6.Tumor_Type_ID AS CN_Navque_Tumor_Type_Id
        --,t1.Lookup_Id
        ,t1.Lookup_Code AS CR_ICD_Oncology_Code
        ,t1.Lookup_Desc AS CR_ICD_Oncology_Site_Desc
        --,t3.ICD_Oncology_Code AS PTID_Primary_ICD_Oncology_Code_Lookup_ID
        --,t3.ICD_Oncology_Site_Desc AS PTID_Primary_ICD_Oncology_Code_Desc
        ,t4.Tumor_Type_Group_Name AS CN_Tumor_Group_Name
        ,t4.Tumor_Type_Desc AS CN_Tumor_Type_Desc
        ,t5.Tumor_Type_Group_Name AS CN_General_Tumor_Group_Name
        ,t5.Tumor_Type_Desc AS CN_General_Tumor_Type_Desc
        ,t6.Tumor_Type_Desc AS CN_NavQue_Tumor_Type_Desc
		,COALESCE(t1.SOURCE_SYSTEM_CODE,t3.SOURCE_SYSTEM_CODE,t4.SOURCE_SYSTEM_CODE,t5.SOURCE_SYSTEM_CODE,t6.SOURCE_SYSTEM_CODE,t7.SOURCE_SYSTEM_CODE) AS SOURCE_SYSTEM_CODE,
		CURRENT_TIMESTAMP(0) as DW_LAST_UPDATE_DATE_TIME 
        FROM 
        (
            --PT ID TumorTypes
            SELECT DISTINCT T4.*  FROM EDWCR_VIEWS.Cancer_Patient_Id_Output t1
            INNER JOIN EDWCR_VIEWS.Ref_ICD_Oncology t4
            ON t4.ICD_Oncology_Code = t1.Submitted_Primary_ICD_Oncology_Code AND t4.ICD_Oncology_Code NOT IN ('c421')
        )t3
        FULL JOIN 
        --CR TumorTypes
        EDWCR_Views.Ref_Lookup_Code t1
        ON SUBSTR(t1.Lookup_Code,1,3) = SUBSTR(t3.ICD_Oncology_Code,1,3)
        LEFT JOIN
        (
            --Navigation Tumor Module  Lookup
            SELECT 
            CASE WHEN t1.Tumor_Type_Desc IN ('Breast') THEN 'C509'
                        WHEN t1.Tumor_Type_Desc IN ('Lung') THEN 'C349'
                        WHEN t1.Tumor_Type_Desc IN ('Hematology') THEN 'C424'
                        WHEN t1.Tumor_Type_Desc IN ('GI Anal') THEN 'C218'
                        WHEN t1.Tumor_Type_Desc IN ('GI Bile Duct') THEN 'C249'
                        WHEN t1.Tumor_Type_Desc IN ('GI Colon') THEN 'C189'
                        WHEN t1.Tumor_Type_Desc IN ('GI Esophageal') THEN 'C159'
                        WHEN t1.Tumor_Type_Desc IN ('GI Gastric') THEN 'C169'
                        WHEN t1.Tumor_Type_Desc IN ('GI Liver') THEN 'C220'
                        WHEN t1.Tumor_Type_Desc IN ('GI Pancreatic') THEN 'C259'
                        WHEN t1.Tumor_Type_Desc IN ('GI Rectal') THEN 'C209'
                        WHEN t1.Tumor_Type_Desc IN ('GI Mixed Tumor') THEN 'C269'
                        END AS Nav_ICD_Xwalk
            ,t1.* 
            FROM EDWCR_Views.Ref_Tumor_Type t1
            WHERE t1.Tumor_Type_Group_Name NOT IN ('General','NavQ','NULL','Other') AND t1.Tumor_Type_Group_Name IS NOT NULL
        )t4
        ON SUBSTR(t1.Lookup_Code,1,3) = SUBSTR(t4.Nav_ICD_Xwalk,1,3)
        LEFT JOIN
        (
            --Navigation General Tumor Look Up
            SELECT
            CASE    WHEN TRIM(t1.Tumor_Type_Desc) IN ('Adrenal') THEN 'C749'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Bone (Osteosarcoma)') THEN 'C419'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Central nervous system (brain, spinal cord)') THEN 'C710'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Cervix') THEN 'C539'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Ewing sarcoma') THEN 'C499'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Eye') THEN 'C699'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Fallopian tube') THEN 'C579'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Kaposi sarcoma') THEN 'C499'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Kidney (renal)') THEN 'C649'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Lacrimal gland') THEN 'C695'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Larynx') THEN 'C329'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Lung') THEN 'C349'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Melanoma') THEN 'C449'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Merkel cell') THEN 'C449'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Mesothelioma (Non-Lung)') THEN 'C809'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Myelodysplasia') THEN 'C424'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Nasal cavity') THEN 'C300'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Neuro endocrine') THEN 'C809'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Oral') THEN 'C069'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Other') THEN 'C809'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Ovary') THEN 'C569'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Parotid gland') THEN 'C079'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Penis') THEN 'C609'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Peritoneum') THEN 'C482'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Pharynx') THEN 'C148'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Placenta') THEN 'C589'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Prostate') THEN 'C619'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Salivary gland') THEN 'C089'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Scrotum') THEN 'C639'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Skin (non melanoma)') THEN 'C449'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Soft tissue') THEN 'C499'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Submandibular gland') THEN 'C779'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Thymus') THEN 'C379'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Testis') THEN 'C629'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Thyroid') THEN 'C739'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Tumor of unknown primary') THEN 'C809'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Ureter') THEN 'C669'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Urethra') THEN 'C689'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Uterine') THEN 'C559'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Vagina') THEN 'C529'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Vulva') THEN 'C519'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Bladder') THEN 'C679'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Breast') THEN 'C509'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Anal') THEN 'C218'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Bile Duct') THEN 'C249'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Colon') THEN 'C189'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Esophageal') THEN 'C159'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Gastric') THEN 'C169'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Liver') THEN 'C220'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Pancreatic') THEN 'C259'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Rectal') THEN 'C209'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('GI Mixed Tumor') THEN 'C269'
                        WHEN TRIM(t1.Tumor_Type_Desc) IN ('Hematology') THEN 'C424'
                        ELSE '-99' END AS Nav_General_ICD_Xwalk
            ,t1.*
            FROM  EDWCR_Views.Ref_Tumor_Type t1
            WHERE t1.Tumor_Type_Group_Name IN ('General')
        )t5
        ON SUBSTR(t1.Lookup_Code,1,3) = SUBSTR(t5.Nav_General_ICD_Xwalk,1,3)
        LEFT JOIN
        (
            --NavQue
            SELECT 
            CASE WHEN  TRIM(t1.Tumor_Type_Desc) IN ('GYN') THEN 'C579'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Lung') THEN 'C349'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Acute Leukemia') THEN 'C424'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Breast') THEN 'C509'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Colon') THEN 'C189'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Sarcoma') THEN 'C499'
                        WHEN  TRIM(t1.Tumor_Type_Desc) IN ('Complex GI') THEN 'C269'
                        END AS NavQ_ICD_Xwalk
            ,t1.* 
            FROM EDWCR_Views.Ref_Tumor_Type t1
            WHERE t1.Tumor_Type_Group_Name IN ('NavQ')
        )t6
        ON SUBSTR(t1.Lookup_Code,1,3) = SUBSTR(t6.NavQ_ICD_Xwalk,1,3)
        LEFT JOIN
        (
            --Tumor Grouping
            SELECT
            CASE WHEN t2.ICD_Oncology_Code IN ('C23','C21','C24','C15','C22','C25','C20','C19','C16','C26','C17') THEN 'Complex GI'
                        WHEN t2.ICD_Oncology_Code IN ('C51','C52','C53','C54','C55','C56','C57') THEN 'Gynecological'
                        END AS Tumor_Group
            ,t2.ICD_Oncology_Code     
			,source_system_code
            FROM
            (
                SELECT DISTINCT
                SUBSTR (t1.ICD_Oncology_Code,1,3) AS ICD_Oncology_Code
                ,t1.ICD_Oncology_Site_Desc
				,t1.source_system_code
                FROM EDWCR_VIEWS.Ref_ICD_Oncology t1 
            )t2
        )t7
        ON SUBSTR(t1.Lookup_Code,1,3) = SUBSTR(t7.ICD_Oncology_Code,1,3)
        
        WHERE t1.Lookup_Id IN ('18')
)  a 
)A"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_REG_CANCER_TUMOR_DRIVER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CANCER_TUMOR_DRIVER"
