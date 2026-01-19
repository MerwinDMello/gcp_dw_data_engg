Select count(1) as mismatch_rows from (
    select 'tgt' src_or_tgt,a.* from (
     select 
      % for col in table_config['columns']:
            % if col['field_type'] == 'STRING':
                  ${col['name']} as ${col['name']}, 
            % else:
                  ${col['name']}, 
            % endif 
      % endfor
      from `${table_config['src_project_id']}.${table_config['src_dataset']}.${table_config['table']}`
      % if 'src_filter' in table_config:
            where ${table_config['src_filter']}
      % endif
      
    except distinct
    
    select  
    % for col in table_config['columns']:
            % if col['field_type'] == 'STRING':
                  ${col['name']} as ${col['name']}, 
            % else:
                  ${col['name']}, 
            % endif 
      % endfor
      from `${table_config['tgt_project_id']}.${table_config['tgt_dataset']}.${table_config['table']}`
      % if 'tgt_filter' in table_config:
            where ${table_config['tgt_filter']}
      % endif 
      ) a
    
    union all
    
    select 'src' src_or_tgt,b.* from (
    select 
      % for col in table_config['columns']:
            % if col['field_type'] == 'STRING':
                  ${col['name']} as ${col['name']}, 
            % else:
                  ${col['name']}, 
            % endif 
      % endfor
      from `${table_config['tgt_project_id']}.${table_config['tgt_dataset']}.${table_config['table']}`
      % if 'tgt_filter' in table_config:
            where ${table_config['tgt_filter']}
      % endif
    
    except distinct
    
    select  
    % for col in table_config['columns']:
            % if col['field_type'] == 'STRING':
                  ${col['name']} as ${col['name']}, 
            % else:
                  ${col['name']}, 
            % endif 
      % endfor
      from `${table_config['src_project_id']}.${table_config['src_dataset']}.${table_config['table']}`
      % if 'src_filter' in table_config:
            where ${table_config['src_filter']}
      % endif
       )  b ) 