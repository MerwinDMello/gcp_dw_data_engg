Select 'J_IM_EPCS_PROVIDER_STATUS_STG'||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
EDWIM_Staging.EPCS_PROVIDER_STATUS_STG;