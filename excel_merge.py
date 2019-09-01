import os
import pandas as pd
import openpyxl
import datetime
import time

# Program parameters here
xl_directory = "dysmetabolism-sheets"
result_name = "merge-result.xlsx"
df_index = "EPIC_MRN"
# Columns that mean the same thing
column_dict = {"NAME": "COMMON_NAME",
               "SIMPLE_GENERIC_NAME": "MEDICATION_NAME",
               "GENERIC_MEDICATION": "MEDICATION_NAME",
               "NOTED DATE": "CONTACT_DATE",
               "EPICMRN": "EPIC_MRN",
               }


def main():
    xls = [filename for filename in os.listdir(os.getcwd() + '/' + xl_directory) if filename.endswith(".xlsx")]
    xls.sort(key=lambda file: os.path.getsize(xl_directory + "/" + file))
    main_df = pd.DataFrame(columns=[df_index]).set_index(df_index)
    start_time = time.time()
    data_processed = 0
    data_array = [os.path.getsize(xl_directory + "/" + file) for file in xls]
    data_total = sum(data_array)

    for index, xl in enumerate(xls):
        t = datetime.datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
        print("\t{} - {}/{} - Running on {}, {}".format(t, index + 1, len(xls), xl, os.path.getsize(xl_directory + "/" + xl)))
        for sheet in openpyxl.load_workbook(xl_directory + "/" + xl).get_sheet_names():
            xl_df = pd.read_excel(xl_directory + "/" + xl, sheet_name=sheet)
            xl_df.columns = [c.upper() for c in xl_df.columns]
            xl_df.rename(columns=column_dict, copy=False, inplace=True)
            if df_index not in xl_df.columns:
                break
            xl_df.drop_duplicates(subset=[df_index], keep='last', inplace=True)
            xl_df.set_index(df_index, inplace=True)
            main_df = pd.concat([main_df, xl_df], axis=1, sort=True, copy=False)
            for column in main_df.columns:
                if type(main_df[column]) == pd.DataFrame:
                    dup_columns = main_df[column]
                    dup_columns.columns = [column + "." * i for i in range(len(main_df[column].columns))]
                    for i in range(1, len(main_df[column].columns)):
                        dup_columns[column] = dup_columns[column].combine_first(dup_columns[column + "." * i])
                    main_df.drop(columns=[column], inplace=True)
                    main_df[column] = dup_columns[column]
        data_total += data_array[index]
        writer = pd.ExcelWriter(result_name, engine='xlsxwriter')
        t2 = datetime.datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
        print("\t{} - saving".format(t2, index + 1, len(xls), xl, os.path.getsize(xl_directory + "/" + xl)))
        main_df.to_excel(writer, openpyxl.load_workbook(result_name).get_sheet_names()[0])
        writer.save()
        writer.close()
    print("-----\ncompleted\n------")


main()

