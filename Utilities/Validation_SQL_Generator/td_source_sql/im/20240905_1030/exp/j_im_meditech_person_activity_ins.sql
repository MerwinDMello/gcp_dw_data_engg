select 'J_IM_Meditech_Person_Activity_Ins' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
 (
SELECT 
T1.IM_DOMAIN_ID,
T1.MT_USER_ID,
T1.ESAF_ACTIVITY_DATE,
T1.ACCESS_RULE_ID,
CASE
            WHEN T1.ACCESS_RULE_ID = 0
            THEN T1.ACCESS_RULE_ID
            ELSE 1
            
END AS IM_PERSON_INACTIVATE_SW,          
'M' AS SOURCE_SYSTEM_CODE ,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
 
 FROM 
        (
			SELECT 
			                S1.IM_DOMAIN_ID,
			                S1.MT_USER_ID,
			                S1.ESAF_ACTIVITY_DATE,
			                CASE
			                
			                
			                			--BEGIN EXCLUSIONS
			                			   WHEN S1.MT_ALIAS_EXEMPT_SW = 1 --EXEMPT ALIASES EXCLUSION RULE  #4 & #5
			                            THEN 0
			                            WHEN S1.MT_EXCLUDED_USER_SW = 1   --EXEPMT MEDITECH CORPORATE USERS EXCLUSION RULE #6
			                            THEN 0
			                            WHEN S1.MT_STAFF_PM_USER_SW = 1  --STAFF PM USER EXCLUSION RULE #8
			                            THEN 0 			                         
			               	            WHEN S4.LAWSON_PERSON_NEW_HIRE_SW = 1   --NEW  LAWSON EMPLOYEE HIRE EXCLUSION RULE #11
                                         THEN 0
                                         WHEN S4.Lawson_Excluded_Job_Class_Sw = 1   --EXEMPT LAWSON JOB CLASS EXCLUSION RULE #12
                                         THEN 0
                                         WHEN S4.Lawson_Excluded_Department_Sw = 1   --EXEMPT LAWSON DEPARTMENTS EXCLUSION RULE #13
                                         THEN 0
                                         WHEN S2.AD_User_New_Sw = 1       --NEW AD NON EMPLOYEE HIRING EXCLUSION RULE #15                                   
                                          THEN 0                                                            
                                           WHEN S2.AD_Group_Exempt_Sw = 1       --EXEMPT AD GROUPS EXCLUSION RULE #16                               
                                          THEN 0
                                          WHEN S3.AD_Employee_Type_Id = 41       --EXEMPT APP AD USER TYPE
                                           THEN 0 
                                          WHEN S3.AD_Employee_Type_Id = 16      --EXEMPT LIP AD USER TYPE
                                           THEN 0
                                           
                                    
                                     
			                            
			                            --BEGIN DISABLEMENTS 
			                            WHEN S1.MT_USER_ACTIVITY_SW = 0  --MIS ACCOUNT WITH SYSTEM LOGON >1 YEAR DISABLMENT RULE #1
			                            THEN 4
			                            WHEN S2.AD_USER_ACTIVE_SW <> 1 --CANNOT BE MATCHED TO AN ENABLED AD ACCOUNT DISABLEMENT RULE #2
			                            THEN 5
			                            WHEN S3.AD_Account_User_Id IS NULL --UNLINKED AD USER ACCOUNT DISABLEMENT RULE #3
			                            THEN 6
			                            
			                            ELSE 0
			                            
			                            
			                   			
			                END AS ACCESS_RULE_ID
			                
			                FROM EDWIM_BASE_VIEWS.MEDITECH_USER_ACTIVITY S1
			                
						    LEFT JOIN EDWIM_BASE_VIEWS.AD_USER_ACTIVITY S2
			                ON S1.MT_USER_ID = S2.AD_ACCOUNT_USER_ID
			                AND S1.ESAF_ACTIVITY_DATE = S2.ESAF_ACTIVITY_DATE
		                
		                   LEFT JOIN EDWIM_BASE_VIEWS.AD_Account S3
		                   ON S1.MT_USER_ID = S3.AD_Account_User_Id		                   
		                   
		                    LEFT JOIN EDWIM_BASE_VIEWS.Lawson_User_Activity s4
               				ON s1.MT_USER_ID = s4.Lawson_Person_User_Id
               			    AND s1.eSAF_Activity_Date = s4.eSAF_Activity_Date
      
                )T1
                
                
WHERE  IM_PERSON_INACTIVATE_SW = 1
)A;