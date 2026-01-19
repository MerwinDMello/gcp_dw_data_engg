
cd /etl/ST/IM/SrcFiles/IM_EDW
export JOBNAME='J_IM_RCOPIA_PROVIDER_STATUS_STG'




export AC_EXP_INPUT_FILE='HCA_Rcopia_Active_Inactive_Providers_trigger.csv'
export P_EXP_Delimiter=' '
export P_EXP_Number_of_Fields='1'
export P_EXP_Control_Total_Field='1,'
export P_EXP_Control_Total_Type='2,'


export AC_ACT_SQL_STATEMENT="Select 'J_IM_RCOPIA_PROVIDER_STATUS_STG'||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
EDWIM_Staging.RCOPIA_PROVIDER_STATUS_STG;"