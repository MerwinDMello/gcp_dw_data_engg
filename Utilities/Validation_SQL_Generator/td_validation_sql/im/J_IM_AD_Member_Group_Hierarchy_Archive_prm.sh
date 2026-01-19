export Job_Name='J_IM_AD_Member_Group_Hierarchy_Archive'
export JOBNAME='J_IM_AD_Member_Group_Hierarchy_Archive'


export AC_EXP_SQL_STATEMENT="Select 'J_IM_AD_Member_Group_Hierarchy_Archive'||','||
cast(coalesce(Count(*),0) as varchar(20))||','
as source_string from

(
select 
	t1.AD_Archive_Date as "AD_Archive_Date",
	t1.AD_Archive_Id as "AD_Archive_Id",
	t1.AD_Domain_Name as "AD_Domain_Name",
	t1.AD_Member_Distinguished_Name as "AD_Member_Distinguished_Name",
	t1.AD_Group_Distinguished_Name as "AD_Group_Distinguished_Name",
	t1.AD_Parent_Group_Distinguished_Name as "AD_Parent_Group_Distinguished_Name",
	t1.AD_User_Name as "AD_User_Name",
	t1.AD_Extract_Load_Date_Time as "AD_Extract_Load_Date_Time",
	t1.AD_Group_Name as "AD_Group_Name",
	t1.AD_Hierarchy_Level_Id as "AD_Hierarchy_Level_Id",
	t1.AD_Parent_Group_Name as "AD_Parent_Group_Name",
	t1.Source_System_Code as "Source_System_Code",
	t1.DW_Last_Update_Date_Time as "DW_Last_Update_Date_Time"
from 
(
select 
		current_date as "AD_Archive_Date",
		row_number() over (order by t2.AD_User_Name, t2.AD_Hierarchy_Level_Id) as "AD_Archive_Id",
		t2.AD_Domain_Name as "AD_Domain_Name",
		t2.AD_Member_Distinguished_Name as "AD_Member_Distinguished_Name",
		t2.AD_Group_Distinguished_Name as "AD_Group_Distinguished_Name",
		t2.AD_Parent_Group_Distinguished_Name as "AD_Parent_Group_Distinguished_Name",
		t2.AD_User_Name as "AD_User_Name",
		t2.AD_Extract_Load_Date_Time as "AD_Extract_Load_Date_Time",
		t2.AD_Group_Name as "AD_Group_Name",
		t2.AD_Hierarchy_Level_Id as "AD_Hierarchy_Level_Id",
		t2.AD_Parent_Group_Name as "AD_Parent_Group_Name",
		t2.Source_System_Code as "Source_System_Code",
		current_timestamp(0) as "DW_Last_Update_Date_Time"
from
	(
		select 
		d.IM_Domain_Name as "AD_Domain_Name",
		m.AD_Member_Distinguished_Name as "AD_Member_Distinguished_Name",
		g.AD_Group_Distinguished_Name as "AD_Group_Distinguished_Name",
		coalesce(p.AD_Group_Distinguished_Name,'') as "AD_Parent_Group_Distinguished_Name",
		m.AD_User_Name as "AD_User_Name",
		h.AD_Extract_Load_Date_Time as "AD_Extract_Load_Date_Time",
		g.AD_Group_Name as "AD_Group_Name",
		h.AD_Hierarchy_Level_Id as "AD_Hierarchy_Level_Id",
		coalesce(p.AD_Group_Name,'') as "AD_Parent_Group_Name",
		m.Source_System_Code as "Source_System_Code"
		from EDWIM.AD_Member_Group_Hierarchy h
		inner join EDWIM.AD_Member_User m on h.AD_Member_Id = m.AD_Member_Id
		inner join EDWIM.AD_Group_User g on h.AD_Group_Id = g.AD_Group_Id
		left outer join EDWIM.AD_Group_User p on h.AD_Group_Parent_Id = p.AD_Group_Id
		inner join EDWIM.Ref_IM_Domain d on m.IM_Domain_Id = d.IM_Domain_Id
		where coalesce(m."AD_User_Name",'') <> ''
	) t2
where t2.AD_User_Name IN 
	(
		select 	distinct
			IM_Person_User_Id
		from EDWIM.IM_Person_Activity
		where eSAF_Activity_Date = current_date
		and Source_System_Code = 'M'
	)
) t1
) SRC;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_AD_Member_Group_Hierarchy_Archive'||','||cast(cast(count(*)as bigint ) as varchar(20)) ||',' as source_string from
EDWIM.AD_Member_Group_Hierarchy_Archive;"

