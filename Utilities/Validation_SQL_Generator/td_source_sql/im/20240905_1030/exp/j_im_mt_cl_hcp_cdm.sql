select 'J_IM_MT_CL_HCP_CDM' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
 
Select distinct
t1.HCP_DW_Id as HCP_DW_Id,
t3.MT_User_Id as MT_User_Id,
Current_Timestamp(0) as time_stamp
From
EDWIM_BASE_VIEWS.Clinical_Health_Care_Provider  t1
INNER JOIN
        (
                SELECT 
                ROLE_PLYR_SK,
                SUBSTR(TRIM(ID_TXT) ,1 ,10) AS NPI
                FROM EDWCDM_BASE_VIEWS.PRCTNR_ROLE_IDFN                
        
WHERE REGISTN_TYPE_REF_CD = 'NPI'
AND VLD_TO_TS = '9999-12-31 00:00:00'
AND ID_TXT <> ''
) t2
ON  CAST(t1.National_Provider_Id AS VARCHAR(10)) = t2.NPI
INNER JOIN 
        (
        SELECT
        ROLE_PLYR_SK,
        SUBSTR(ID_TXT,10, 7) AS MT_User_Id
        
        FROM EDWCDM_BASE_VIEWS.PRCTNR_ROLE_IDFN
        
        WHERE REGISTN_TYPE_REF_CD = 'LOGON_ID'
        AND CHAR_LENGTH(TRIM(MT_User_Id)) = 7 
        AND REGEXP_INSTR(SUBSTR(TRIM(MT_User_Id) ,4 ,4), '[A-Za-z_]') = 0
        AND REGEXP_INSTR(SUBSTR(TRIM(MT_User_Id) ,1 ,3), '[0-9_]') = 0) t3
ON t2.ROLE_PLYR_SK = t3.ROLE_PLYR_SK
WHERE t1.National_Provider_Id IS NOT NULL
AND t1.National_Provider_Id > 0
QUALIFY ROW_NUMBER() OVER ( PARTITION BY t1.HCP_DW_Id
  ORDER BY t3.MT_User_Id DESC) = 1
        
)A;