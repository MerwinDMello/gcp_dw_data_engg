create table if not exists `{{ params.param_hr_core_dataset_name }}.company_pay_schedule`
(
  company_pay_schedule_sid INT64 NOT NULL OPTIONS(description="it is etl generated unqiue identifier for combination of schedule, schedule flag , effective data, and company"),
  eff_from_date DATE NOT NULL OPTIONS(description="record effective start date"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="date on which the record became valid. load date typically."),
  valid_to_date DATETIME OPTIONS(description="date on which the record was invalidated."),
  hr_company_sid INT64 NOT NULL OPTIONS(description="unique id for the number that represents an established company and is entered on all functions"),
  lawson_company_num INT64 NOT NULL OPTIONS(description="the number that identifies a company.a company represents a business or legal entity of an organization"),
  pay_schedule_code STRING NOT NULL OPTIONS(description="contains a set of steps and grades for a step and grade schedule, or a set of grades and minimum, midpoint, andmaximum values for a grade range schedule"),
  pay_schedule_flag STRING NOT NULL OPTIONS(description="indicates whether this schedule is a step and grade schedule or a grade range schedule. you cannot use the same schedule name for both kinds of schedules. g = grade range s = step and grade"),
  pay_schedule_eff_date DATE NOT NULL OPTIONS(description="contains the date when the step and grade or grade range schedule goes into effect"),
  pay_schedule_desc STRING OPTIONS(description="contains the user-defined description of the schedule"),
  salary_class_flag STRING OPTIONS(description="indicates if the table is used for salaried or hourly schedules. h = hourly s = salaried"),
  last_grade_sequence_num INT64 OPTIONS(description="contains the last number used for a grade sequence number"),
  pay_schedule_status_ind INT64 OPTIONS(description="contains the status of a given transaction. 0 = new schedule - must run pr110, 1 = changed schedule - run ,2 = current schedule - updated, 3 = no longer in use - history, 5 = grade range schedule"),
  currency_code STRING OPTIONS(description="currency in which salary is credited"),
  currency_nd INT64 OPTIONS(description="the number of decimals assigned to a  particular account as determined by the account currency."),
  active_dw_ind STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  process_level_code STRING NOT NULL OPTIONS(description="unique process level code of an hr company value mainatined in this field."),
  security_key_text STRING NOT NULL OPTIONS(description="it is the key which has concatenation of lawson company code - process level code -department code for data access feature prospective. "),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY company_pay_schedule_sid, eff_from_date
OPTIONS(
  description="this table maintains unique list of  pay schedule details. pay schedule may be based on grade level or grade and step level. "
);