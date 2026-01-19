export Job_Name='J_IM_Ref_AD_Employee_Type'
export JOBNAME='J_IM_Ref_AD_Employee_Type'
export col_name='AD_Employee_Type_Id'
export tab_name='EDWIM.Ref_AD_Employee_Type'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Ref_AD_Employee_Type' + ',' +
coalesce(cast(ltrim(rtrim(A.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(
select count(distinct employeeType) as counts from  dbo.ActiveDirectoryUsers where coalesce(employeeType,'') <> ''
) A;" 


export AC_ACT_SQL_STATEMENT="select 'J_IM_Ref_AD_Employee_Type' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_AD_Employee_Type                
)A;"

