#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_MT_CL_USER_LAST_ACTIVITY'
export JOBNAME='J_IM_MT_CL_USER_LAST_ACTIVITY'

export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_CL_USER_LAST_ACTIVITY' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(

SELECT 
t1.Company_Code as company_code
,t1.Coid as coid
,t1.MT_User_Mnemonic_CS as MT_User_Mnemonic_CS
,t1.Network_Mnemonic_CS as Network_Mnemonic_CS
,t1.MT_User_Last_Activity_Date as MT_User_Last_Activity_Date
,CURRENT_TIMESTAMP(0)  as time_stamp
FROM 
        (
                SELECT  
                Company_Code as company_code,
                Coid as coid,
                MT_User_Mnemonic_CS as MT_User_Mnemonic_CS,
                Network_Mnemonic_CS as Network_Mnemonic_CS,
                MT_User_Last_Activity_Date as MT_User_Last_Activity_Date
                
                FROM EDWIM_STAGING.MT_CL_User_Activity
                
                UNION ALL
                
                SELECT  
		Company_Code as company_code,
                Coid as coid,
                MT_User_Mnemonic_CS as MT_User_Mnemonic_CS,
                Network_Mnemonic_CS as Network_Mnemonic_CS,
                MT_User_Last_Activity_Date as MT_User_Last_Activity_Date
                
                FROM EDWIM_STAGING.MT_CL_User_Activity_Hist
        ) t1          
QUALIFY ROW_NUMBER() OVER(PARTITION BY t1.Network_Mnemonic_CS, t1.MT_User_Mnemonic_CS ORDER BY MT_User_Last_Activity_Date DESC) = 1
        
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_CL_USER_LAST_ACTIVITY' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CL_User_Last_Activity         
           
           )A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#

