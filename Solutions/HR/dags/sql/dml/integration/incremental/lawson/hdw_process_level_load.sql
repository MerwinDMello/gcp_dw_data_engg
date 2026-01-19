begin declare dup_count int64;

declare current_ts datetime;

declare lv_par string;

set
  lv_par = "trim(coalesce(cast(company as string),''))  ||\'-\'||  case when trim(coalesce(process_level ,''))  = ''  then \'00000\' else trim(process_level) end";

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

call `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  '{{ params.param_hr_stage_dataset_name }}',
  'prsystem',
  lv_par,
  'Process_Level'
);

truncate table {{ params.param_hr_stage_dataset_name }}.process_level_wrk;

insert into
  {{ params.param_hr_stage_dataset_name }}.process_level_wrk (
    process_level_sid,
    eff_from_date,
    hr_company_sid,
    lawson_company_num,
    process_level_code,
    process_level_name,
    process_level_active_code,
    eff_to_date,
    active_dw_ind,
    security_key_text,
    source_system_code,
    dw_last_update_date_time,
    row_id
  )
select
  cast(process_level_sid as int64),
  date(current_ts),
  hr_company_sid,
  lawson_company_num,
  process_level_code,
  process_level_name,
  process_level_active_code,
  eff_to_date,
  active_dw_ind,
  security_key_text,
  source_system_code,
  current_ts,
  row_id
from
  (
    select
      xwlk.sk as process_level_sid,
      current_ts as eff_from_date,
      coalesce(hr.hr_company_sid, 0) as hr_company_sid,
      prs.company as lawson_company_num,
      case
        when upper(trim(coalesce(prs.process_level, ''))) = '' then '00000'
        else trim(prs.process_level)
      end as process_level_code,
      prs.name as process_level_name,
      prs.active_flag as process_level_active_code,
      cast(
        coalesce(
          max(prs.date_stamp) over (
            partition by prs.company,
            prs.process_level
            order by
              prs.date_stamp,
              prs.time_stamp rows between 1 following
              and 1 following
          ) - interval 1 second,
          date '9999-12-31'
        ) as date
      ) as eff_to_date,
      case
        when row_number() over (
          partition by prs.company,
          prs.process_level
          order by
            prs.date_stamp desc,
            prs.date_stamp desc
        ) = 1 then 'Y'
        else 'N'
      end as active_dw_ind,
      concat(
        substr(
          '00000',
          1,
          5 - length(trim(cast(prs.company as string)))
        ),
        trim(cast(prs.company as string)),
        '-',
        case
          when trim(prs.process_level) is null
          or trim(prs.process_level) = '' then '00000'
          else concat(
            substr('00000', 1, 5 - length(trim(prs.process_level))),
            trim(prs.process_level)
          )
        end,
        '-',
        '00000'
      ) as security_key_text,
      'L' as source_system_code,
      current_ts as dw_last_update_date_time,
      row_number() over (
        partition by prs.company,
        prs.process_level
        order by
          prs.date_stamp desc,
          prs.date_stamp desc
      ) as row_id
    from
      {{ params.param_hr_stage_dataset_name }}.prsystem as prs
      inner join {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk as xwlk on upper(
        substr(
          concat(
            trim(coalesce(cast(prs.company as string), '')),
            '-',
            case
              when upper(trim(coalesce(prs.process_level, ''))) = '' then '00000'
              else trim(prs.process_level)
            end
          ),
          1,
          255
        )
      ) = upper(xwlk.sk_source_txt)
      and upper(xwlk.sk_type) = 'PROCESS_LEVEL'
      left outer join (
        select
          hr_company.hr_company_sid,
          hr_company.lawson_company_num
        from
          {{ params.param_hr_base_views_dataset_name }}.hr_company
        where
          date(hr_company.valid_to_date) = '9999-12-31'
          and upper(hr_company.source_system_code) = 'L'
        group by
          1,
          2
      ) as hr on (prs.company) = (hr.lawson_company_num)
      left outer join {{ params.param_hr_base_views_dataset_name }}.process_level as tgt on xwlk.sk = tgt.process_level_sid
      and upper(xwlk.sk_type) = 'PROCESS_LEVEL'
      and upper(tgt.active_dw_ind) = 'Y'
      and upper(tgt.source_system_code) = 'L'
  );

begin transaction;

update
  {{ params.param_hr_core_dataset_name }}.process_level as tgt
set
  valid_to_date = date_sub(current_ts, interval 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
from
  {{ params.param_hr_stage_dataset_name }}.process_level_wrk as stg
where
  (tgt.process_level_sid) = (stg.process_level_sid)
  and trim(tgt.source_system_code) = trim(stg.source_system_code)
  and (
    (tgt.hr_company_sid) <> (stg.hr_company_sid)
    or upper(trim(coalesce(tgt.process_level_code, ''))) <> upper(trim(coalesce(stg.process_level_code, '')))
    or upper(trim(coalesce(tgt.process_level_name, ''))) <> upper(trim(coalesce(stg.process_level_name, '')))
  )
  and upper(tgt.active_dw_ind) = 'Y'
  and date(tgt.valid_to_date) = date '9999-12-31'
  AND tgt.source_system_code = 'L';

update
  {{ params.param_hr_core_dataset_name }}.process_level as prlv
set
  valid_to_date = date_sub(current_ts, interval 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
from
  (
    select
      process_level_wrk.process_level_sid,
      process_level_wrk.hr_company_sid,
      process_level_wrk.process_level_code,
      process_level_wrk.process_level_name,
      process_level_wrk.process_level_active_code,
      process_level_wrk.eff_from_date,
      process_level_wrk.source_system_code
    from
      {{ params.param_hr_stage_dataset_name }}.process_level_wrk
    where
      process_level_wrk.row_id = 1
  ) as stg
where
  (prlv.process_level_sid) = (stg.process_level_sid)
  and prlv.source_system_code = stg.source_system_code
  and (
    (prlv.hr_company_sid) <> (stg.hr_company_sid)
    or upper(trim(coalesce(prlv.process_level_code, ''))) <> upper(trim(coalesce(stg.process_level_code, '')))
    or upper(trim(coalesce(prlv.process_level_name, ''))) <> upper(trim(coalesce(stg.process_level_name, '')))
    or upper(
      trim(coalesce(prlv.process_level_active_code, ''))
    ) <> upper(
      trim(coalesce(stg.process_level_active_code, ''))
    )
  )
  and upper(prlv.active_dw_ind) = 'Y'
  and date(prlv.valid_to_date) = date '9999-12-31'
  AND prlv.source_system_code = 'L';

insert into
  {{ params.param_hr_core_dataset_name }}.process_level (
    process_level_sid,
    valid_from_date,
    hr_company_sid,
    lawson_company_num,
    process_level_code,
    process_level_name,
    process_level_active_code,
    active_dw_ind,
    valid_to_date,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
select
  process_level_wrk.process_level_sid,
  current_ts,
  process_level_wrk.hr_company_sid,
  process_level_wrk.lawson_company_num,
  process_level_wrk.process_level_code,
  process_level_wrk.process_level_name,
  process_level_wrk.process_level_active_code,
  process_level_wrk.active_dw_ind,
  datetime("9999-12-31 23:59:59"),
  process_level_wrk.security_key_text,
  process_level_wrk.source_system_code,
  current_ts
from
  {{ params.param_hr_stage_dataset_name }}.process_level_wrk
where
  (
    trim(
      cast(process_level_wrk.process_level_sid as string)
    ),
    trim(cast(process_level_wrk.hr_company_sid as string)),
    upper(
      trim(
        coalesce(process_level_wrk.process_level_code, '')
      )
    ),
    upper(
      trim(
        coalesce(process_level_wrk.process_level_name, '')
      )
    )
  ) not in(
    select
      as struct trim(cast(process_level.process_level_sid as string)),
      trim(cast(process_level.hr_company_sid as string)),
      upper(
        trim(coalesce(process_level.process_level_code, ''))
      ),
      upper(
        trim(coalesce(process_level.process_level_name, ''))
      )
    from
      {{ params.param_hr_base_views_dataset_name }}.process_level
    where
      upper(process_level.active_dw_ind) = 'Y'
      and date(valid_to_date) = date '9999-12-31'
  );

set
  dup_count = (
    select
      count(*)
    from
      (
        select
          process_level_sid,
          valid_from_date
        from
          {{ params.param_hr_core_dataset_name }}.process_level
        group by
          process_level_sid,
          valid_from_date
        having
          count(*) > 1
      )
  );

if dup_count <> 0 then rollback transaction;

raise using message = concat(
  'duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.process_level '
);

else commit transaction;

end if;

end;