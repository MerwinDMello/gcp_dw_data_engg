select 
'TMP' AS Artiva_Instance
,replace(STU.USERID,' ', '') as USERID
,substr(STU.HCUADEPT,1,6) as HCUADEPT --User's Department or Group
,substr(STU.HCUADEPT,1,3) as Department
,trim(STU.UAFULLNAME) as UAFULLNAME --User's Full Name
,'v_currtimestamp'  as DW_Last_Update_Date_Time
from SQLUser.STUSER STU