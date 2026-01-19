delete from edwhr_staging.ats_cust_v3_jobapplication_stg where substr(cast(candidate as string),1,1)='-';
