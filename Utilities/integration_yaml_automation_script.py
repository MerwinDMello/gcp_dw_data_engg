import pandas as pd
import os

def read_excel(file_path, sheet_name):
    return pd.read_excel(file_path, sheet_name=sheet_name)

def read_pattern(file_path):
    with open(file_path, "r") as pattern_file:
        return pattern_file.read()\

def get_filename_from_path(path):
    return os.path.basename(path)

def rank_records(df):
    df['rank']=df.groupby('BTEQ/Shell Script').cumcount() + 1
    return df

def write_output(df, pattern, output_directory,source_system,schedule,source_types,frequency,BTEQScripts):
    print("--Columns--")
    print(df.columns)
    
    df_filtered = df[(df['Step Type'].str.upper().isin(['STAGING', 'CORE'])) & (df['rank'] == 1)]
    unique_sourcesystems = df_filtered['Migration'].unique()
    
    heading_template = '''
- source_system: {Source_system}
  frequency: {Frequency}
  start_date: pendulum.datetime(2023, 4, 6, tz=timezone)
  schedule: {Schedule}
  type: {source_type}
  done_file: donefile_name_YYYYMMDD.txt
  dependency:
'''

    for sourcesystem in unique_sourcesystems:
        # Filter the DataFrame for each unique sourcesystem
        df_sourcesystem = df_filtered[df_filtered['Migration'] == sourcesystem]


        # Sort the DataFrame by 'jobid', 'rank', and 'step' columns
        df_sorted = df_sourcesystem.sort_values(by=['Job','Step','rank'])

        unique_groups = df_sorted.groupby([source_system,schedule,source_types,frequency])


        output_file = os.path.join(output_directory, f'{sourcesystem}_integrate_dependency-automated.yaml'.lower())
        print(output_file)


        with open(output_file, "w") as txt_file:

            txt_file.write("integrate:\n")
            for (Source_system, Schedule,source_type,Frequency), group in unique_groups:
                heading = heading_template.format(Schedule=Schedule, Source_system=Source_system.lower(),source_type=source_type,Frequency=Frequency.lower())
                txt_file.write(heading)
                
                for _, row in group.iterrows():
                    SQL_filename=get_filename_from_path(row[BTEQScripts])
                    txt_file.write(pattern.format(SQL_file=SQL_filename.lower(), Target_table_modified=row['Target table'].split('.')[1].lower()) + "\n")

def main():
    # Input Output Details
    excel_file = ""
    excel_sheet = ""
    pattern_file = ""
    output_directory = ""

    # Excel Columns
    source_system=""
    schedule=""
    source_types=""
    frequency=""
    BTEQScripts=""


    df = read_excel(excel_file,excel_sheet)
    # print(df)
    df_ranked = rank_records(df)

    pattern = read_pattern(pattern_file)
    write_output(df_ranked, pattern, output_directory,source_system,schedule,source_types,frequency,BTEQScripts)

    print("Text files generated successfully.")

if __name__ == "__main__":
    main()
