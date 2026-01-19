SELECT 'J_CR_Rad_Onc_Patient_Course'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from edwcr_staging.STG_DimCourse dp
inner join edwcr.Ref_Rad_Onc_Site rs 
on rs.Source_Site_Id=dp.DimSiteID
LEFT JOIN edwcr.Rad_Onc_Patient ra 
on dp.DimPatientID = ra.Source_Patient_Id 
and rs.Site_Sk=ra.Site_SK 