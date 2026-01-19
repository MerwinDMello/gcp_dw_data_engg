select 'J_IM_MEDITECH_USER' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
 (
SELECT DISTINCT 
t6.IM_Domain_Id  AS IM_Domain_Id,
t4.MT_User_Id,   
SUBSTR(TRIM(t3.ID_TXT) ,1 ,10) AS National_Provider_Id,
t2.FCLT_MNEM_CD AS  Facility_Mnemonic_CS,
t1.FL_NM AS MT_User_Full_Name,
SUBSTR(TRIM(t5.ID_TXT) ,1 ,10)  AS MT_User_Mnemonic_CS,
t1.PRCTNR_ROLE_CD AS MT_User_Page_1_Provider_Type_Desc,
t2.PRCTNR_FCLT_ROLE_CD AS MT_User_Page_2_Provider_Type_Desc,
CASE
        WHEN t1.ACTV_IND = 'Y'
        THEN CASE
                            WHEN t2.PRCTNR_FCLT_ROLE_CD IN ('1hcaResdnt', '1hcaFellow', '1hcaStuden', 
                                                                                            '1hcaPharm', '2hcaActive', '2hcaAsocAf', '2hcaAmbul', '2hcaCourts', 
                                                                                            '2hcaConsul', '2hcaPrvNoM', '2hcaNonPrv', '2hcaHOLD')
                            THEN 'Y'
                            ELSE 'N'
                       END
        ELSE 'N'
END AS MT_User_Exempt_Sw,
t1.ACTV_IND AS MT_User_Active_Ind,
SUBSTR(TRIM(t7.ID_TXT) ,1 ,10)  AS MT_User_MIS_User_Mnemonic,
t8.MEDITECH_LST_LOGON_DT AS MT_User_Last_Activity_Date,
'M' AS Source_System_Code,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM  EDWIM_BASE_VIEWS.PRCTNR_DTL t1
INNER JOIN 
        (
                SELECT 
                s1.ROLE_PLYR_SK,
                s1.VLD_FR_TS,
                s1.COMPANY_CODE,
                s1.COID,
                s1.FCLT_MNEM_CD,
                s2.Network_Mnemonic_CS,
                s1.PRCTNR_FCLT_ROLE_CD
                
                FROM  EDWIM_BASE_VIEWS.PRCTNR_FCLT_DTL s1
                
                INNER JOIN 
                        (
                                SELECT 
                                Company_Code,
                                Coid,
                                Network_Mnemonic_CS
                                
                                FROM EDW_PUB_VIEWS.Clinical_Facility
                                
                                WHERE Company_Code = 'H'
                                AND NOT((Network_Mnemonic_CS) = '')
                                AND  NOT(TRIM(Network_Mnemonic_CS) = '1SQI' )
                                AND NOT(Facility_Mnemonic_CS = '')
                                AND NOT(SUBSTR(TRIM(Facility_Mnemonic_CS), 6, 1) = '_')
                                AND NOT(SUBSTR(TRIM(Facility_Mnemonic_CS), 7, 1)  = '_')
                        ) s2
                ON s1.Company_Code = s2.Company_Code
                AND s1.Coid = s2.Coid
                
                WHERE s1.COMPANY_CODE = 'H'
                AND s1.VLD_TO_TS =  '9999-12-31 00:00:00'
        ) t2
ON t1.ROLE_PLYR_SK  = t2.ROLE_PLYR_SK
INNER JOIN EDWIM_BASE_VIEWS.Ref_IM_Domain t6  --Network
ON  t2.Network_Mnemonic_CS = t6.IM_Domain_Name
AND t6.Application_System_Id IN (5, 6)
INNER JOIN 
    (
                SELECT
                ROLE_PLYR_SK,
                SUBSTR(ID_TXT,10, 7) AS MT_User_Id
                
                FROM EDWCDM_BASE_VIEWS.PRCTNR_ROLE_IDFN
                
                WHERE REGISTN_TYPE_REF_CD = 'LOGON_ID'  -- User_Id (3/4)
                AND CHAR_LENGTH(TRIM(MT_User_Id)) = 7 
                AND REGEXP_INSTR(MT_User_Id, '[*.()_]') = 0
                AND REGEXP_INSTR(SUBSTR(TRIM(MT_User_Id) ,4 ,4), '[A-Za-z_]') = 0
                AND REGEXP_INSTR(SUBSTR(TRIM(MT_User_Id) ,1 ,3), '[0-9_]') = 0
                AND VLD_TO_TS = '9999-12-31 00:00:00'
        )  t4 --User_ID (3/4)
ON t1.ROLE_PLYR_SK = t4.ROLE_PLYR_SK
LEFT JOIN EDWIM_Base_Views.PRCTNR_ROLE_IDFN t3
ON t1.ROLE_PLYR_SK = t3.ROLE_PLYR_SK
AND t3.REGISTN_TYPE_REF_CD = 'NPI' --NPI (CMS)
AND t3.VLD_TO_TS <> '9999-12-31 00:00:00'
LEFT JOIN  EDWIM_BASE_VIEWS.PRCTNR_ROLE_IDFN t5
ON t1.ROLE_PLYR_SK = t5.ROLE_PLYR_SK
AND t5.REGISTN_TYPE_REF_CD = 'MNEMONIC'  -- Provider_Mnemonic
AND  t5.VLD_TO_TS = '9999-12-31 00:00:00'
AND  t5.ID_TXT <> ''
LEFT JOIN EDWIM_BASE_VIEWS.PRCTNR_ROLE_IDFN t7  
ON  t1.ROLE_PLYR_SK = t7.ROLE_PLYR_SK
AND t7.REGISTN_TYPE_REF_CD = 'USER_MNE' -- User Mnemonic
AND t7.VLD_TO_TS = '9999-12-31 00:00:00'
AND t7.ID_TXT <> ''
LEFT JOIN EDWIM_Base_Views.PRCTNR_ACTVT_DTL t8 --Last Activity Date
ON  t2.ROLE_PLYR_SK = t8.ROLE_PLYR_SK
AND t2.Network_Mnemonic_CS = t8.NTWK_MNEM_CS
AND CURRENT_DATE - t8.MEDITECH_LST_LOGON_DT <= 365
AND t8.VLD_TO_TS = '9999-12-31 00:00:00'
QUALIFY ROW_NUMBER() OVER(PARTITION BY t6.IM_Domain_Id ,	t4.MT_User_Id  --Remove Duplicates
                                                                      ORDER BY MT_User_Exempt_Sw DESC,
                                                                                                 t8.MEDITECH_LST_LOGON_DT DESC) = 1 
        
WHERE t1.ACTV_IND = 'Y'
AND t1.VLD_TO_TS =  '9999-12-31 00:00:00'
)A;