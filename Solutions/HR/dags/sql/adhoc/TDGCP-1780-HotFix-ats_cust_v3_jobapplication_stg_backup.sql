create or replace table edwhr_staging.ats_cust_v3_jobapplication_stg_orig as (
        select *
        from edwhr_staging.ats_cust_v3_jobapplication_stg
    );