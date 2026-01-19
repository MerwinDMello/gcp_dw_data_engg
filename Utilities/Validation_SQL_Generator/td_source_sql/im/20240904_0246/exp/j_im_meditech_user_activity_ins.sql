select 'J_IM_Meditech_User_Activity_Ins' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
 (
SELECT 
T1.IM_DOMAIN_ID,
T1.MT_USER_ID,
CURRENT_DATE AS ESAF_ACTIVITY_DATE,
T1.MT_USER_LAST_ACTIVITY_DATE,
T1.MT_USER_ACTIVITY_SW,
T1.MT_EXCLUDED_USER_SW,
T1.MT_STAFF_PM_USER_SW,
T1.MT_ALIAS_EXEMPT_SW,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM 
			(
			
			
			SELECT DISTINCT 
			
			S1.IM_DOMAIN_ID,
			S1.MT_USER_ID,
			
			    CASE                 
                                WHEN s5.HPF_User_Last_Activity_Date IS NULL
                                	THEN S4.PK_User_Last_Activity_Date
                                WHEN S4.PK_User_Last_Activity_Date IS NULL
                                	THEN  s5.HPF_User_Last_Activity_Date
                                WHEN s5.HPF_User_Last_Activity_Date >= S4.PK_User_Last_Activity_Date
                                	THEN s5.HPF_User_Last_Activity_Date
                                ELSE S4.PK_User_Last_Activity_Date                                                                     
                END AS Other_Platform_Activity_Date,          
                                                 
                CASE
                                WHEN s1.MT_User_Last_Activity_Date IS NULL
                                	THEN Other_Platform_Activity_Date
                                 WHEN Other_Platform_Activity_Date IS NULL
                                	THEN  s1.MT_User_Last_Activity_Date
                                 WHEN s1.MT_User_Last_Activity_Date >= Other_Platform_Activity_Date
                                 	THEN s1.MT_User_Last_Activity_Date
                                 ELSE Other_Platform_Activity_Date
			END AS MT_USER_LAST_ACTIVITY_DATE,
			
			    CASE 
                            WHEN MT_USER_LAST_ACTIVITY_DATE IS NULL
                            	THEN 0
                            WHEN (CURRENT_DATE - MT_USER_LAST_ACTIVITY_DATE) > 365
                            	THEN 0
                            ELSE 1
                END AS MT_USER_ACTIVITY_SW,  --SYSTEM LOGON > 1 YEAR DISABLEMENT RULE #1
                
                CASE 
                		WHEN S1.MT_USER_MNEMONIC_CS IN ('1PDSRB0154', '1PDJRH1716','1PDMMW6006')
                			THEN 1
                		ELSE 0
                END AS MT_EXCLUDED_USER_SW, --CORPORATE EXEMPT USERS EXCLUSION RULE #6
                
                CASE
                		WHEN S2.NTLOGIN IS NOT NULL
                			THEN 1
                		ELSE 0
                END AS MT_STAFF_PM_USER_SW,  --STAFF PM USERS EXCLUSION RULE #8
                
                CASE
                		WHEN S3.PRCTNR_MNEM_CS IS NOT NULL
                			THEN 1
                		ELSE 0 
                END AS MT_ALIAS_EXEMPT_SW  --MT EXEMPT ALIASES EXCLUSION RULE #4 & #5
                
              
			
			
			 
			FROM EDWIM_BASE_VIEWS.MEDITECH_USER S1
			
			LEFT
			JOIN  EDWIM_STAGING.STAFF_PM_USERS S2
			   ON  S1.MT_USER_ID = S2.NTLOGIN
			   
			LEFT
			JOIN
			
							(							
														
										SELECT 
										T1.IM_DOMAIN_ID,
										T2.NTWK_MNEM_CS, 
										T2.PRCTNR_MNEM_CS,
										T2.PRCTNR_ALIAS_NM 
										
										FROM EDWIM_BASE_VIEWS.REF_IM_DOMAIN T1
										
										INNER JOIN 
										        (
										                SELECT DISTINCT 
										                
										                NTWK_MNEM_CS, 
										                TRIM(PRCTNR_MNEM_CS) PRCTNR_MNEM_CS,
										                PRCTNR_ALIAS_NM,
										                CASE 
										                        WHEN PRCTNR_ALIAS_NM = 'INTERFACE'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'NON-HCA FACILITY'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OPO ORGAN PROCUREMENT'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER – CPOE'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER – DISASTER'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER - DISASTER ID'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER – DOWNTIME'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'OTHER – ROBOT'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'PAGER'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'PAGER – PERSON'    
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'PHONE'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'SCRIPT'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'TEMPLATE'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'TEMPLATE - PARALLON SC'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'TEMPLATE – PBPG'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'TEMPLATE – PWS'  
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'TRACKER'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER – CORPORATE'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER – EVS'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER – FAN'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER – PARALLON'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER - PARALLON CODING'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER - PARALLON HEALTHPORT'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER - PARALLON TECHNOLOGY SOLUTIONS'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER - PARALLON WORKFORCE SOLUTIONS'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER – PWS'
										                        THEN 1
										                        WHEN PRCTNR_ALIAS_NM = 'USER - PWS LSC'
										                        THEN 1
										                       WHEN PRCTNR_ALIAS_NM = 'USER – SCRI, VENDOR'
										                        THEN 1
										                        ELSE 0
										                END AS MT_USER_EXMPT                    
										                
										                FROM EDWIM_BASE_VIEWS.PRCTNR_ACTVT_DTL
										
										                WHERE   MT_USER_EXMPT  = 1                
										        )T2
										        
										ON T2.NTWK_MNEM_CS =  T1.IM_DOMAIN_NAME 
							
							
									) S3 
							
							ON S1.IM_DOMAIN_ID = S3.IM_DOMAIN_ID
						 AND S1.MT_USER_MNEMONIC_CS = S3.PRCTNR_MNEM_CS
			    						
			    						
			    		LEFT 
			    		JOIN EDWIM_BASE_VIEWS.PK_USER S4
			    		   ON S1.IM_DOMAIN_ID = S4.IM_DOMAIN_ID
			    	    AND S1.MT_USER_ID = S4.PK_USER_ID
			    	    
			    	    LEFT
			    	    JOIN
							    	      
				                (    
				                              SELECT DISTINCT
				                                
					                               h2.MT_Domain_Id,					                                                        
					                               h1.HPF_User_Id,
					                               h1.HPF_User_Last_Activity_Date                                        
				                                
				                                FROM EDWIM_Base_Views.HPF_Account  h1
				                                
				                               INNER JOIN  EDWIM_BASE_VIEWS.Platform_Domain_Xwalk h2
				                               ON h1.IM_Domain_Id = h2.MT_Domain_Id
				                               
				                 ) s5
				                ON   s1.MT_User_Id = s5.HPF_User_Id
				                AND  s1.IM_Domain_Id  =  s5.MT_Domain_Id
			    	  
			    	    
			    	  
			
			)T1
)A;