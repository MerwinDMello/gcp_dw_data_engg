export JOBNAME='J_REF_NAVIGATION_MEASURE'


#######Expected Parameter###################

export P_EXP_Control_Total_Field='1'
export P_EXP_Number_of_Fields='4'
export P_EXP_Control_Total_Type='1'
export AC_EXP_INPUT_FILE="Ref_Navigation_Measure.txt"
export P_EXP_Delimiter='|'
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0


export AC_ACT_SQL_STATEMENT="select 'J_REF_NAVIGATION_MEASURE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Navigation_Measure"
