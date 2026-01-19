SELECT 'J_CR_Rad_Onc_Patient_Plan'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from edwcr_staging.STG_DimPlan dp
inner join edwcr.Ref_Rad_Onc_Plan_Purpose rpp 
on dp.PlanIntent=rpp.Plan_Purpose_Name
Inner join  edwcr.Ref_Rad_Onc_Site rr  
on rr.Source_Site_Id = dp.DimSiteID
Left outer join edwcr.Rad_Onc_Patient_Course rpc 
on rpc.Source_Patient_Course_Id = dp.DimCourseID  
Left Join edwcr.Rad_Onc_Patient_Plan core
ON rr.Site_SK = Core.Site_SK
AND dp.DimPlanID = Core.Source_Patient_Plan_Id
where core.Patient_Plan_SK is null