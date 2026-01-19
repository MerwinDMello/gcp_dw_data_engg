Select 'J_IM_RCOPIA_PROVIDER_STATUS_STG'||','||cast(zeroifnull(count(*)) as varchar(20)) ||',' as source_string from
EDWIM_Staging.RCOPIA_PROVIDER_STATUS_STG;