with deduped_landing_src as ( 
      select 
      % for col in table_config['columns']:
            ${col['name']},
      % endfor
      row_number() over (
                        partition by ${table_config['key_column']} 
                        % if 'order_by' in table_config:
                              order by ${table_config['order_by']}
                        % endif
                        ) as rn 
      from `${table_config['src_project_id']}.${table_config['src_dataset']}.${table_config['table']}`
      % if 'src_filter' in table_config:
            where ${table_config['src_filter']}
      % endif
) ,

deduped_landing_tgt as ( 
      select
      % for col in table_config['columns']:
            ${col['name']},
      % endfor
      row_number() over (
                        partition by ${table_config['key_column']}  
                        % if 'order_by' in table_config:
                              order by ${table_config['order_by']}
                        % endif
                        ) as rn 
      from `${table_config['tgt_project_id']}.${table_config['tgt_dataset']}.${table_config['table']}`
      % if 'tgt_filter' in table_config:
            where ${table_config['tgt_filter']}
      % endif
) 

select 
      % for col in table_config['columns']:
      sum(case when 
            % if col['field_type'] == 'STRING':
                  TRIM(UPPER(a.${col['name']})) = TRIM(UPPER(b.${col['name']}))
            % else:
                  a.${col['name']} = b.${col['name']}
            % endif 
            or 
            ( a.${col['name']} is null and b.${col['name']} is null ) 
            then 0 else 1 end
      ) as non_matching_${col['name']},
      % endfor
from deduped_landing_tgt as a full outer join deduped_landing_src as b 
on a.${table_config['key_column']} = b.${table_config['key_column']}  
and b.rn=1 and a.rn=1