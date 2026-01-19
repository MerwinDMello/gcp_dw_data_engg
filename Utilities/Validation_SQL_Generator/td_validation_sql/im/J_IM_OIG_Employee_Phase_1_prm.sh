#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_IM_OIG_Employee_Phase_1'
export FILE_DATE=`date +%d-%b-%Y`
export TARGET_DIR=/etl/ST/IM/TgtFiles/IM_EDW
export TGTFILE='OIG-HCA-E-01-'${FILE_DATE}'.csv'

export AC_EXP_SQL_STATEMENT="select 'J_IM_OIG_Employee_Phase_1'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_Views.OIG_Employee_Phase_1;" 

export AC_ACT_INPUT_FILE='OIG-HCA-E-01-'${FILE_DATE}'.csv'
export P_ACT_Delimiter=','
export P_ACT_Number_of_Fields='11'
export P_ACT_Control_Total_Field='1,'
export P_ACT_Control_Total_Type='1,'




#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#
