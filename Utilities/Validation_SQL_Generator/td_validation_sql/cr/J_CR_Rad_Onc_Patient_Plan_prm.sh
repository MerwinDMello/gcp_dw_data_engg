export JOBNAME='J_CR_Rad_Onc_Patient_Plan'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from $NCR_STG_SCHEMA.STG_DimPlan dp

inner join $NCR_TGT_SCHEMA.Ref_Rad_Onc_Plan_Purpose rpp 
on dp.PlanIntent=rpp.Plan_Purpose_Name

Inner join  $NCR_TGT_SCHEMA.Ref_Rad_Onc_Site rr  
on rr.Source_Site_Id = dp.DimSiteID

Left outer join $NCR_TGT_SCHEMA.Rad_Onc_Patient_Course rpc 
on rpc.Source_Patient_Course_Id = dp.DimCourseID  

Left Join $NCR_TGT_SCHEMA.Rad_Onc_Patient_Plan core
ON rr.Site_SK = Core.Site_SK
AND dp.DimPlanID = Core.Source_Patient_Plan_Id

where core.Patient_Plan_SK is null"

export AC_ACT_SQL_STATEMENT="select '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Rad_Onc_Patient_Plan
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


