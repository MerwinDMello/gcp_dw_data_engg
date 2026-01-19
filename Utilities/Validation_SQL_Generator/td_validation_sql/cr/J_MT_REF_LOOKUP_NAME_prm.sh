export JOBNAME='J_MT_REF_LOOKUP_NAME'


#######Expected Parameter###################

export P_EXP_Control_Total_Field='1'
export P_EXP_Number_of_Fields='2'
export P_EXP_Control_Total_Type='1'
export AC_EXP_INPUT_FILE="Ref_Lookup_Name.txt"
export P_EXP_Delimiter='|'
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0


export AC_ACT_SQL_STATEMENT="select 'J_MT_REF_LOOKUP_NAME' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from $NCR_TGT_SCHEMA.Ref_Lookup_Name"
