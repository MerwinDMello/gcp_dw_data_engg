SELECT 'REF_RAD_ONC_PLAN_PURPOSE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM
(
Select distinct
Case When Trim(DHD.PlanIntent)='' Then Null ELSE Trim(DHD.PlanIntent) END as Plan_Purpose_Name,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from EDWCR_STAGING.stg_DimPlan DHD
)src