select  
( select count(1)
  from `${table_config['src_project_id']}.${table_config['src_dataset']}.${table_config['table']}`
  % if 'src_filter' in table_config:
    where ${table_config['src_filter']}
  % endif
) as source, 

( 
  select count(1)
  from `${table_config['tgt_project_id']}.${table_config['tgt_dataset']}.${table_config['table']}`
  % if 'tgt_filter' in table_config:
    where ${table_config['tgt_filter']}
  % endif
) as target