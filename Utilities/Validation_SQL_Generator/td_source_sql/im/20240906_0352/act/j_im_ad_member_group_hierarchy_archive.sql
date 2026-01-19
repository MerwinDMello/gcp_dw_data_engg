select 'J_IM_AD_Member_Group_Hierarchy_Archive'||','||cast(cast(count(*)as bigint ) as varchar(20)) ||',' as source_string from
EDWIM.AD_Member_Group_Hierarchy_Archive;