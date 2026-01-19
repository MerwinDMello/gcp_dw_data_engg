select 'J_IM_MT_CL_HCP_VC' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(
 
Select Distinct
t1.HCP_DW_Id as HCP_DW_Id,
t2.HCP_User_Id_3_4 as HCP_User_Id_3_4,
Current_timestamp(0) as time_stamp
From EDWIM_BASE_VIEWS.Clinical_Health_Care_Provider t1
inner join
      (
                SELECT DISTINCT
                t2.HCP_Other_Id AS HCP_User_Id_3_4,
                t1.HCP_NPI
                
                FROM EDWIM_BASE_VIEWS.HCP t1
                
                INNER  JOIN EDWIM_BASE_VIEWS.HCP_Other_Id t2
                ON t1.HCP_DW_Id = t2.HCP_DW_Id
                AND t2.Id_Type_Sid = 17248
                AND t2.HCP_Other_ID_Active_Ind = 'Y'
                AND CHAR_LENGTH(TRIM(t2.HCP_Other_Id)) = 7 
                AND REGEXP_INSTR(SUBSTR(TRIM(t2.HCP_Other_Id) ,4 ,4), '[A-Za-z_]') = 0
                AND REGEXP_INSTR(SUBSTR(TRIM(t2.HCP_Other_Id) ,1 ,3), '[0-9_]') = 0
                
                WHERE t1.HCP_Active_Ind = 'Y'
                AND (NOT(t1.HCP_Last_Name = '')
                AND NOT(t1.HCP_First_Name = ''))
                AND t2.HCP_Other_Id <> ''
                QUALIFY ROW_NUMBER() OVER ( PARTITION BY t1.HCP_NPI ORDER BY t2.HCP_Other_Id) = 1
             
        ) t2
ON t1.National_Provider_Id = t2.HCP_NPI
WHERE NOT(t1.National_Provider_Id IS NULL) AND 
      NOT (t1.National_Provider_Id = '')
)A;