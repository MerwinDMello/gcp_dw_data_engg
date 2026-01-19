-- inserting missing data into target table for snapshot_date '2023-05-10'
-- deleting any data if there is miss/ or druplicate run of the dag to avoid any duplicates in data
delete from {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep_snapshot
where snapshot_date = '2023-05-10';

insert into {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep_snapshot
select * from
hca-hin-dev-cur-hr.edwhr.candidate_pathway_microstep_snapshot
where snapshot_date = '2023-05-10';