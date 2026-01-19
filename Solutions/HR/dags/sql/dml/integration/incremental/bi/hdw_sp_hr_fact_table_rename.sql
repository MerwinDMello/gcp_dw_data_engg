--#####################################################################################
--#                                                                                   #
--# Target Table - {{ params.param_hr_core_dataset_name }}.Fact_HR_Metric                 #
--#                                                                                   #
--# CHANGE CONTROL:                                                                   #
--#                                                                                   #
--# DATE          Developer     Change Comment                                        #
--# 02/18/2021    M LaFever     Initial Version                                       #
--#                                                                                   #
--#                                                                                   #
--# Renames {{ params.param_hr_core_dataset_name }}.Fact_HR_Metric_MV to {{ params.param_hr_core_dataset_name }}.Fact_HR_Metric
--#####################################################################################

ALTER TABLE {{ params.param_hr_core_dataset_name }}.fact_hr_metric RENAME TO fact_hr_metric_old;
ALTER TABLE {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv RENAME TO fact_hr_metric;
ALTER TABLE {{ params.param_hr_core_dataset_name }}.fact_hr_metric_old RENAME TO fact_hr_metric_mv;

