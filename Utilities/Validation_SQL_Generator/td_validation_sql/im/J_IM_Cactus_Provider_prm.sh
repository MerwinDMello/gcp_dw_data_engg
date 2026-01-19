#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_Cactus_Provider'
export JOBNAME='J_IM_Cactus_Provider'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Cactus_Provider' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
 (
SELECT

		t7.IM_Domain_Id,
		SUBSTR(t3.HCP_Other_Id , 1, 7) AS Cactus_Provider_User_Id,
		t1.HCP_Src_Sys_Key,
		t1.HCP_NPI,			
		t6.Fac_Asgn_Stts_Sid,
		t6.Fac_Asgn_Stts_Src_Sys_Key,
		t2.Prov_Cat_Sid,
		t2.Prov_Cat_Src_Sys_Key,		
		t1.HCP_First_Name, 
		t1.HCP_Last_Name,	
		t1.HCP_Middle_Name,
		CASE
			WHEN t6.Fac_Asgn_Stts_Desc IN ('Current','Temporary')
				THEN 1
			ELSE 0
		END AS Cactus_Provider_Activity_Exempt_Sw,		
		'K' AS Source_System_Code,
         CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
FROM	EDWIM_Base_Views.HCP t1

JOIN EDWIM_Base_Views.Ref_Provider_Category t2
	ON t1.HCP_Cat_Sid = t2.Prov_Cat_Sid
	AND  t2.Prov_Cat_Src_Sys_Key IN ('D2G019AL2J','D2G019AKNU')

JOIN EDWIM_Base_Views.HCP_Other_Id t3
	ON t1.HCP_DW_Id = t3.HCP_DW_Id

JOIN EDWIM_Base_Views.Ref_Id_Type t4
	ON t3.Id_Type_Sid = t4.Id_Type_Sid
	AND t4.Id_Type_Src_Sys_Key = 'D2QK0IXBT7'


JOIN EDWIM_Base_Views.HCP_Facility_Assignment t5
	ON t1.HCP_DW_Id = t5.HCP_DW_Id
	AND t5.Fac_Asgn_Active_Ind = 'Y'

JOIN EDWIM_Base_Views.Ref_Facility_Asgn_Status t6
	ON  t5.Fac_Asgn_Stts_Sid = t6.Fac_Asgn_Stts_Sid 
	AND t6.Entity_Sid = 1

JOIN EDWIM_STAGING.HPF_Instance_Facility_Xwalk t7
	ON t5.COID = t7.COID
	AND t5.Company_Code = t7.Company_Code
       
       QUALIFY ROW_NUMBER() OVER(PARTITION BY t7.IM_Domain_Id,	Cactus_Provider_User_Id
                                                                                ORDER BY t5.HCP_Fac_Asgn_Eff_To_Date DESC) = 1 

WHERE t1.HCP_Active_Ind = 'Y'
	AND Cactus_Provider_User_Id IS NOT NULL
	AND NOT(TRIM(Cactus_Provider_User_Id) = '')
	AND REGEXP_INSTR(Cactus_Provider_User_Id, '[.]') = 0
	AND REGEXP_INSTR(SUBSTR(TRIM(Cactus_Provider_User_Id) ,4 ,4), '[A-Za-z_]') = 0
	AND REGEXP_INSTR(SUBSTR(TRIM(Cactus_Provider_User_Id) ,1 ,3), '[0-9_]') = 0
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_Cactus_Provider' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM EDWIM.Cactus_Provider        
           
           )A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#



 