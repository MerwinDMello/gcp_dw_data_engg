select 
HolidayCalendarId as Holiday_Calendar_Id,
HolidayCalendarDate as Holiday_Calendar_Date ,
COALESCE(HolidayCalendarDescr,'') as Holiday_Calendar_Desc,
cast(CreatedDate as datetime) as Created_Date_Time,
COALESCE(CreatedBy,'') as Created_By_3_4_Id,
cast(UpdatedDate as datetime) as Source_Last_Update_Date_Time,
COALESCE(UpdatedBy,'') as Updated_By_3_4_Id,
case when IsActive =1 then 'Y' else 'N' end as Active_Ind,
'N' as Source_System_Code
,'v_currtimestamp' as DW_Last_Update_Date_Time
from Discover.dbo.HolidayCalendar;