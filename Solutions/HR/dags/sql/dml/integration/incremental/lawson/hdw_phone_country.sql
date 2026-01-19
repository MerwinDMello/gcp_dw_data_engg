declare ts datetime;

declare dup_count int64;

set
  ts = datetime_trunc(current_datetime('US/Central'), second);

begin create temp table phone_country_tmp as (
  SELECT
    X.Country_Code,
    Y.country_name
  FROM
    (
      SELECT
        TRIM(Wk_Phone_Cntry) AS Country_Code
      from
        {{ params.param_hr_stage_dataset_name }}.paemployee
      UNION
      DISTINCT
      SELECT
        TRIM(Hm_Phone_Cntry) AS Country_Code
      from
        {{ params.param_hr_stage_dataset_name }}.paemployee
      UNION
      DISTINCT
      SELECT
        TRIM(Wk_Phone_Cntry) AS Country_Code
      from
        {{ params.param_hr_stage_dataset_name }}.pajobreq
      UNION
      DISTINCT
      SELECT
        TRIM(Phone_Country) AS Country_Code
      from
        {{ params.param_hr_stage_dataset_name }}.pcodesdtl
    ) X
    left outer join {{ params.param_hr_stage_dataset_name }}.phone_country_stg Y on substring(x.country_code,1,3) = y.country_code
  WHERE
    LENGTH(trim(X.Country_Code)) > 0
);

update
  phone_country_tmp
set
  country_name = 'Unknown'
where
  country_name is null;

begin transaction;

merge {{ params.param_hr_core_dataset_name }}.phone_country tgt using phone_country_tmp src on tgt.country_code = src.country_code
when matched then
update
set
  tgt.country_name = src.country_name,
  tgt.source_system_code = 'L',
  tgt.dw_last_update_date_time = ts
  when not matched then
insert
(
    country_code,
    country_name,
    source_system_code,
    dw_last_update_date_time
  )
values
(country_code, country_name, 'L', ts);

SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          country_code
        from
          {{ params.param_hr_core_dataset_name }}.phone_country
        group by
          country_code
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: phone_country'
);

ELSE COMMIT TRANSACTION;

END IF;

end;