##



export Job_Name='J_OPOL_Ref_Holiday_Calendar'






export AC_EXP_SQL_STATEMENT="Select 'J_OPOL_Ref_Holiday_Calendar' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from Discover.dbo.HolidayCalendar"

export AC_ACT_SQL_STATEMENT="Select 'J_OPOL_Ref_Holiday_Calendar'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR.Ref_Holiday_Calendar"