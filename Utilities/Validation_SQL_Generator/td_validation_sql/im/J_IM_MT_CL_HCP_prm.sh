
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export Job_Name='J_IM_MT_CL_HCP'
export JOBNAME='J_IM_MT_CL_HCP'

export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_CL_HCP' ||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from 
(


Select 
t1.Company_Code as Company_Code,
t1.Coid as  Coid,
t1.HCP_Mnemonic_CS as HCP_Mnemonic_CS,
COALESCE(t1.HCP_MIS_User_Mnemonic, '') AS HCP_MIS_User_Mnemonic,
t1.Network_Mnemonic_CS as Network_Mnemonic_CS,
t1.National_Provider_Id as National_Provider_Id,
COALESCE(t2.HCP_User_Id, t3.HCP_User_Id) AS HCP_User_Id_3_4,
t1.HCP_Full_Name as HCP_Full_Name,
Current_Timestamp(0) as time_stamp

From
EDWIM_BASE_VIEWS.Clinical_Health_Care_Provider  t1


Left Outer join
EDWIM_STAGING.MT_CL_HCP_CDM t2
ON t1.HCP_DW_Id = t2.HCP_DW_Id


Left Outer join
EDWIM_STAGING.MT_CL_HCP_VC t3
ON t1.HCP_DW_Id = t3.HCP_DW_Id

WHERE NOT(t1.HCP_Mnemonic_CS = 'UNDEFINED ')
AND (NOT(HCP_User_Id_3_4 IS NULL)
AND NOT (HCP_User_Id_3_4 = ''))

QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.HCP_DW_Id, t1.HCP_Mnemonic_CS
                            ORDER BY  t1.HCP_MIS_User_Mnemonic DESC, HCP_User_Id_3_4 DESC) = 1
        
)A;"



export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_CL_HCP' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
 FROM Edwim_Staging.MT_CL_HCP        
           
           )A;"






#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#


