import copy
import os.path
import csv
import pandas as pd
import numpy as np
import time
import re
from sqlalchemy import create_engine
import cx_Oracle
import psycopg2
import warnings
warnings.filterwarnings("ignore")

# input excel file path
inputFile = os.getcwd() + "\\input_directory_xl\\input_1.xlsx"

# Reading an excel file
excelFile = pd.read_excel(inputFile, sheet_name=None)

# Converting excel file into CSV file
for key in excelFile.keys():
    excelFile[key].to_csv("input_directory_csv/" + ("%s.csv" % key), index=None, header=True)

csvfile_names = list(excelFile.keys())

first_db = 'POSTGRES'
second_db = 'ORACLE'
user_name = 'testuser'
localhost = 'localhost'
pswd = 'testuser'
db_name = "dbopmipr"
flag = "db"

input_main_path = "input_directory_csv/Main.csv"
input_keys_path = "input_directory_csv/Keys.csv"
input_exclude_path = "input_directory_csv/Ignored Columns.csv"

def connection_pg():
    db_connection = psycopg2.connect(host=localhost, database=db_name, user=user_name, password=pswd, port='5432')
    db_cursor1 = db_connection.cursor()
    return db_cursor1

def connection_ora():
    dsn_tns = cx_Oracle.makedsn('Localhost', '9480', service_name='oratest')
    db_connect = cx_Oracle.connect(user='testuser', password='testuser', dsn=dsn_tns)
    db_cursor2 = db_connect.cursor()
    return db_cursor2

def pg_db(pg_cursor, file):
    pg_cursor.execute('SELECT * FROM ' + file)
    table1_rows = pg_cursor.fetchall()
    table1_title = [i[0] for i in pg_cursor.description]
    return table1_rows, table1_title

def ora_db(ora_cursor, file):
    ora_cursor.execute('SELECT * FROM ' + file)
    table2_rows = ora_cursor.fetchall()
    table2_title = [i[0] for i in ora_cursor.description]
    return table2_rows, table2_title

def compare_rows(row1, row2, header1):
    message = ""
    if len(row1) != len(row2):
        message = "value length didnt match"
        return message
    else:
        for index in range(len(row1)):
            if not message:
                message = "Values didn't match for "
            if row1[index] != row2[index]:
                message += str(header1[index]) + " and "

        return message[:-5]

def exclude_col(dataframes, excludeCol_data, excludeCol_elements, pk_elements):
    pk_list_val_lower = [each.lower() for each in list(pk_elements)]
    lower_exclude_col = [each.lower() for each in excludeCol_elements]
    lower_exclude_col_data = excludeCol_data.lower()
    # pk and exclude must not be same
    res = [check_val for check_val in lower_exclude_col if check_val in pk_list_val_lower]
    if len(res) >= 1:
        raise Exception("Unable to perform")
    else:
        dataframes.columns = [x.lower() for x in dataframes.columns]
        data_after_excluding = (dataframes.loc[:, dataframes.columns != lower_exclude_col_data])
    return data_after_excluding

def pk_merged_data(index_list, all_headers_table, csv_data, headers_lowercase):
    number_to_header = []

    for each_ind in index_list:
        if each_ind.isnumeric() or re.findall(r"[-+]?\d*\.*\d+", each_ind):
            float_convert = float(each_ind)
            ind = int(float_convert)
            number_to_header.append(all_headers_table[ind - 1])
        else:
            number_to_header.append(each_ind)
    pk = ""
    for each_header in number_to_header:

        if True:
            ind = headers_lowercase.index(each_header.casefold())
            ind = ind + 1
        pk += str(csv_data[ind])

    return pk

def drop_tables(table_name):
    mycursor = connection_ora()
    mycursor.execute("BEGIN"
                     "    EXECUTE IMMEDIATE 'DROP TABLE \"" + table_name + "\"';"
                                                                           "EXCEPTION"
                                                                           "    WHEN OTHERS THEN"
                                                                           "        IF SQLCODE != -942 THEN"
                                                                           "            RAISE;"
                                                                           "        END IF;"
                                                                           "END;")
    mycursor.execute("BEGIN"
                     "    EXECUTE IMMEDIATE 'DROP TABLE " + table_name + "';"
                                                                         "EXCEPTION"
                                                                         "    WHEN OTHERS THEN"
                                                                         "        IF SQLCODE != -942 THEN"
                                                                         "            RAISE;"
                                                                         "        END IF;"
                                                                         "END;")

def drop_tables_if_exists(first_file, second_file):
    if "." in first_file:
        res_1 = first_file.partition(".")
        list_od_res = list(res_1)
        first_file = list_od_res[2]

    if "." in second_file:
        res_2 = second_file.partition(".")
        list_od_res_2 = list(res_2)
        second_file = list_od_res_2[2]

    summery_report = "SUMMARY_PG_TO_ORA"
    # drop_tables(first_file + "_post")
    # drop_tables(second_file + "_post")
    # drop_tables("matched_records" + first_file + second_file)
    drop_tables("PG_" + first_file + "_VS_ORA_" + second_file)
    drop_tables("PG_" + first_file + "_NOT_IN_ORA_" + second_file)
    drop_tables("ORA_" + second_file + "_NOT_IN_PG" + first_file)
    drop_tables(summery_report)

def create_tables_db(engine, first_file, second_file, a_post_val_df, b_post_val_df, common_val_df, non_matched_vals_df,
                     a_notin_b_vals_df, b_notin_a_vals_df):
    if "." in first_file:
        res_1 = first_file.partition(".")
        list_od_res = list(res_1)
        first_file = list_od_res[2]

    if "." in second_file:
        res_2 = second_file.partition(".")
        list_od_res_2 = list(res_2)
        second_file = list_od_res_2[2]

    print(first_file, second_file)
    # if not a_post_val_df.empty:
    #     with engine.connect() as conn, conn.begin():
    #         a_post_val_df.to_sql("PG_"+first_file + "_post", conn, if_exists='append', index=False)
    #

    # if not b_post_val_df.empty:
    #     with engine.connect() as conn, conn.begin():
    #         b_post_val_df.to_sql("ORA_"+second_file + "_post", conn, if_exists='append', index=False)
    #

    # if not common_val_df.empty:
    #     with engine.connect() as conn, conn.begin():
    #         common_val_df.to_sql("matched_records_PG_" + first_file +"_AND_ORA_"+ second_file, conn,
    #                              if_exists='append',
    #                              index=False)
    #

    if not non_matched_vals_df.empty:
        with engine.connect() as conn, conn.begin():
            non_matched_vals_df.to_sql("PG_"+ first_file + "_VS_ORA_" + second_file, conn, if_exists='append',
                                       index=False)

    if not a_notin_b_vals_df.empty:
        with engine.connect() as conn, conn.begin():
            a_notin_b_vals_df.to_sql("PG_"+first_file + "_NOT_IN_ORA_" + second_file, conn, if_exists='append',
                                     index=False)

    if not b_notin_a_vals_df.empty:
        with engine.connect() as conn, conn.begin():
            b_notin_a_vals_df.to_sql("ORA_"+second_file + "_NOT_IN_PG_" + first_file, conn, if_exists='append',
                                     index=False)

def create_summery(output):
    engine = create_engine('oracle+cx_oracle://testuser:testuser@localhost:9480/?service_name=oratest')
    if not output.empty:
        with engine.connect() as conn, conn.begin():
            output.to_sql("SUMMARY_PG_TO_ORA", conn, if_exists='append', index=False)
    else:
        print("Summary table not created!!!")
        pass

def _cast_type_difference(df1, df2):
    if set(df1.columns) != set(df2.columns):
        raise Exception("List of columns didn't match")

    for col in df1.columns:
        # float/int64/timestamp conversion
        if df1[col].dtype == df2[col].dtype:
            continue
        elif 'int64' in [df1[col].dtype, df2[col].dtype]:
            df1[col].fillna(0, inplace=True)
            df2[col].fillna(0, inplace=True)
            df1 = df1.astype({col: 'int64'})
            df2 = df2.astype({col: 'int64'})
        elif 'float64' in [df1[col].dtype, df2[col].dtype]:
            print(col)
            df1[col].fillna(0, inplace=True)
            df2[col].fillna(0, inplace=True)
            df1 = df1.astype({col: 'float64'})
            df2 = df2.astype({col: 'float64'})
        elif 'datetime64[ns]' in [df1[col].dtype, df2[col].dtype]:
            df1[col].fillna('', inplace=True)
            df2[col].fillna('', inplace=True)
            df1 = df1.astype({col: 'datetime64[ns]'})
            df2 = df2.astype({col: 'datetime64[ns]'})

    return df1, df2


#handle null in dataframe
def _handlenullvalue(df):
    #impute datetime missing value
    for col in df.columns[(df.dtypes == 'datetime64[ns]')]:
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else '')

    # imput null for bool cols
    for col in df.columns[(df.dtypes == 'bool')]:
        df[col].fillna(0, inplace=True)
        df[col].replace(to_replace=np.nan, value=0, inplace=True)

    # imput null for int cols
    for col in df.columns[(df.dtypes == 'int64')]:
        df[col].fillna(0, inplace=True)
        df[col].replace(to_replace=np.nan, value=0, inplace=True)

    # impute null for float cols
    for col in df.columns[(df.dtypes == 'float64')]:
        df[col].fillna(0.0, inplace=True)
        df[col].replace(to_replace=np.nan, value=0.0, inplace=True)

    #impute missing data for object columns
    for col in df.columns[(df.dtypes == 'object')]:
        df[col].fillna('', inplace=True)
        df[col].replace(to_replace=np.nan, value='', inplace=True)
        df[col].replace(regex=[r'^\ +$', r'^\t+$', r'^\n+$', 'None', 'N/A', 'n/a', 'NaT'], value='', inplace=True)

    return df

def comparision():
    print(time.ctime())
    print("Comparison start ...")
    table_not_exists = []
    table_not_exists_comment = []
    output_file = []
    summery_result = []
    input_csv = pd.read_csv(input_main_path)
    comparision_check = "COMPARE_NEEDED"
    Primary_KEYS_check = "KEYS_AVAILABLE"
    Exclude_col_check = "COLUMN_NEED_TO_IGNORE"

    # ----------------------------------------------compare_check ----------------------------------------------------------------
    inputCSV_header = list(input_csv.columns)

    if os.path.exists("SUMMARY_PG_TO_ORA.csv"):
        os.remove("SUMMARY_PG_TO_ORA.csv")

    pk_set = set()

    def check_required_or_not(check):
        check_res = ""
        find_indx_pos = inputCSV_header.index(check)
        res_val = str(input_csv_data[find_indx_pos]).split(" ")
        check_res += res_val[0]
        return check_res

    for input_csv_data in input_csv.itertuples(index=False):
        try:
            first_file = input_csv_data[1]
            second_file = input_csv_data[0]
            index_list = []
            # ============================================check comparision ===============================
            compare_check_res = ""
            if comparision_check in input_csv:
                compare_check_res += check_required_or_not(comparision_check)
            if compare_check_res.casefold() == "YES".casefold():
                if flag == "db":
                    pg_cursor = connection_pg()
                    ora_cursor = connection_ora()
                    try:
                        data_1, title_table_1 = pg_db(pg_cursor, first_file)
                        table_df_1 = pd.DataFrame(data_1)

                    except Exception:
                        print(Exception)
                        table_not_exists.append(first_file + " VS " + second_file)
                        table_not_exists_comment.append(first_file + " not available in " + first_db)

                    try:
                        data_2, title_table_2 = ora_db(ora_cursor, second_file)
                        table_df_2 = pd.DataFrame(data_2)
                    except Exception:
                        table_not_exists.append(first_file + " VS " + second_file)
                        table_not_exists_comment.append(second_file + " not available in " + second_db)

                elif flag == "local":
                    try:
                        first_file_path ="PG/"+ first_file + ".csv"
                        table_df_1 = pd.read_csv(first_file_path, low_memory=False, encoding='latin-1')
                        title_table_1 = table_df_1.columns.tolist()
                    except Exception:
                        table_not_exists.append("PG/"+ first_file + " VS " + second_file)
                        table_not_exists_comment.append(first_file + " not available for Comparison")
                    try:
                        second_file_path = "ORA/"+second_file + ".csv"
                        table_df_2 = pd.read_csv(second_file_path, low_memory=False, encoding='latin-1')
                        title_table_2 = table_df_2.columns.values.tolist()
                    except Exception:
                        table_not_exists.append("ORA/"+ first_file + " VS " + second_file)
                        table_not_exists_comment.append(second_file + " not available for Comparison ")
                else:
                    data_1, title_table_1, table_df_1 = [], [], []
                    data_2, title_table_2, table_df_2 = [], [], []

                all_headers_table_1 = copy.deepcopy(title_table_1)
                all_headers_table_2 = copy.deepcopy(title_table_2)

                if table_df_1.empty:
                    table_not_exists.append(first_file + " VS " + second_file)
                    table_not_exists_comment.append(first_file + " Table is Empty In " + first_db)
                elif table_df_2.empty:
                    table_not_exists.append(first_file + " VS " + second_file)
                    table_not_exists_comment.append(second_file + " Table is Empty In " + second_db)

                table_df_1.set_axis(title_table_1, axis=1, inplace=True)
                table_df_2.set_axis(title_table_2, axis=1, inplace=True)

                a_post_header = title_table_1
                b_post_header = title_table_2

                df_1 = table_df_1
                df_2 = table_df_2

                # ------------------------------------------------------pk index_part ------------------------------------------------------------
                primary_key_check_res = ""
                if Primary_KEYS_check in input_csv:
                    primary_key_check_res += check_required_or_not(Primary_KEYS_check)

                    if primary_key_check_res.casefold() == "YES".casefold():
                        # opening new Index primary key csv File

                        dict_val = {}

                        input_pk_csv_file = pd.read_csv(input_keys_path)
                        for each_key in input_pk_csv_file.itertuples(index=False):
                            key_df = pd.DataFrame(each_key)
                            if not key_df.empty:
                                dict_val[key_df[0][0]] = (key_df[0][1:].tolist())

                        for each_val in dict_val:
                            if first_file == each_val:
                                pk_table_names = dict_val[each_val]
                                new_list = [item for item in pk_table_names if not (pd.isnull(item)) == True]
                                for each_table in new_list:
                                    index_list.append(str(each_table))

                                if len(index_list) > 0 and '0' not in index_list and "nan" not in index_list and '0.0' not in index_list:

                                    for each_ind in index_list:
                                        if each_ind.isnumeric() or re.findall(r"[-+]?\d*\.*\d+", each_ind):
                                            float_convert = float(each_ind)
                                            ind = int(float_convert)
                                            pk_set.add(title_table_1[ind - 1])

                                        else:
                                            if each_ind in title_table_1:
                                                ind = title_table_1.index(each_ind)
                                                ind = ind + 1
                                                pk_set.add(each_ind)

                                            else:
                                                ind = title_table_2.index(each_ind)
                                                ind = ind + 1
                                                pk_set.add(each_ind)
                    else:
                        print("NO PrimaryKeys available , it can Compare  String Based {} and  {}".format(first_file,
                                                                                                          second_file))

                exclude_col_check_res = ""
                if Exclude_col_check in input_csv:
                    exclude_col_check_res += check_required_or_not(Exclude_col_check)

                    if exclude_col_check_res.casefold() == "YES".casefold():
                        dict_val_exclude = {}

                        input_eclude_csv_file = pd.read_csv(input_exclude_path)
                        for each_exclude_key in input_eclude_csv_file.itertuples(index=False):
                            exclude_col_df = pd.DataFrame(each_exclude_key)
                            if not exclude_col_df.empty:
                                dict_val_exclude[exclude_col_df[0][0]] = (exclude_col_df[0][1:].tolist())

                        for each_val in dict_val_exclude:
                            if first_file == each_val:
                                exclude_names = dict_val_exclude[each_val]
                                exclude_header = [item for item in exclude_names if not (pd.isnull(item)) == True]
                                exclude_col_numbers = []
                                if len(exclude_header) >= 1:
                                    exclude_ele_convert_str = []
                                    for convert_str_ele in exclude_header:
                                        if convert_str_ele == 0 or convert_str_ele == 0.0:
                                            pass
                                        elif type(convert_str_ele) == float or type(convert_str_ele) == int:
                                            if type(convert_str_ele) == float:
                                                val_add = int(convert_str_ele)
                                            else:
                                                val_add = int(convert_str_ele)
                                            exclude_ele_convert_str.append(str(val_add))
                                        else:
                                            exclude_ele_convert_str.append(str(convert_str_ele))

                                    for exclude_data_col in exclude_ele_convert_str:
                                        if exclude_data_col.isdigit():
                                            exclude_col_numbers_str_to_int = int(exclude_data_col)
                                            exclude_col_numbers.append(
                                                title_table_1[exclude_col_numbers_str_to_int - 1])
                                            table_df_1 = exclude_col(table_df_1,
                                                                     title_table_1[exclude_col_numbers_str_to_int - 1],
                                                                     exclude_col_numbers, pk_set)
                                            table_df_2 = exclude_col(table_df_2,
                                                                     title_table_2[exclude_col_numbers_str_to_int - 1],
                                                                     exclude_col_numbers, pk_set)
                                        else:
                                            exclude_col_numbers.append(exclude_data_col)
                                            table_df_1 = exclude_col(table_df_1, exclude_data_col, exclude_col_numbers,
                                                                     pk_set)
                                            table_df_2 = exclude_col(table_df_2, exclude_data_col, exclude_col_numbers,
                                                                     pk_set)

                                    df_1, df_2 = _cast_type_difference(table_df_1, table_df_2)
                                    df_1=_handlenullvalue(df_1)
                                    df_2=_handlenullvalue(df_2)
                                    a_post_header.clear()
                                    b_post_header.clear()

                                    header_A = df_1.columns.values
                                    a_post_header.extend(header_A)

                                    header_B = df_2.columns.values
                                    b_post_header.extend(header_B)

                                else:
                                    pass

                df_1 = _handlenullvalue(df_1)
                df_2 = _handlenullvalue(df_2)

                df_1.index += 1
                df_2.index += 1

                engine = create_engine('oracle+cx_oracle://testuser:testuser@localhost:9480/?service_name=oratest')

                csv1_mapping_dict = {}
                csv2_mapping_dict = {}

                all_col_names_lowercase_table_1 = []
                all_col_names_lowercase_table_2 = []
                for each_col_name in title_table_1:
                    all_col_names_lowercase_table_1.append(each_col_name.casefold())
                for each_col_name in title_table_2:
                    all_col_names_lowercase_table_2.append(each_col_name.casefold())

                for csv1_data in df_1.itertuples():

                    if primary_key_check_res.casefold() == "YES".casefold():  # DF_1  ---> pk is available --> Now PKBASED Compare

                        if len(index_list) > 0 and '0' not in index_list and "nan" not in index_list and '0.0' not in index_list:
                            pk_1 = pk_merged_data(index_list, all_headers_table_1, csv1_data,
                                                  all_col_names_lowercase_table_1)
                            csv1_mapping_dict[pk_1] = list(csv1_data)

                    else:  # --> string_Based Compare

                        ax = []
                        pk = ""
                        for each_1 in csv1_data:
                            each_ele_str_1 = str(each_1)
                            ax.append(each_ele_str_1)
                        pk += ",".join(ax[1::])
                        csv1_mapping_dict[pk] = list(csv1_data)

                for csv2_data in df_2.itertuples():
                    if primary_key_check_res.casefold() == "YES".casefold():  # DF_2  ---> pk is available --> Now PKBASED Compare

                        if len(index_list) > 0 and '0' not in index_list and "nan" not in index_list:
                            pk_2 = pk_merged_data(index_list, all_headers_table_2, csv2_data,
                                                  all_col_names_lowercase_table_2)
                            csv2_mapping_dict[pk_2] = list(csv2_data)

                    else:  # --> string_Based Compare

                        ax = []
                        pk = ""
                        for each_2 in csv2_data:
                            each_ele_str_2 = str(each_2)
                            ax.append(each_ele_str_2)
                        pk += ",".join(ax[1::])
                        csv2_mapping_dict[pk] = list(csv2_data)

                a_post_val = []
                b_post_val = []
                A_not_B = []
                B_not_A = []
                non_matched_vals = []
                common_val = []

                for each_key in csv1_mapping_dict:
                    if each_key in csv2_mapping_dict:

                        csv1_mapping_dict_key_cp = copy.deepcopy(csv1_mapping_dict[each_key])
                        csv2_mapping_dict_key_cp = copy.deepcopy(csv2_mapping_dict[each_key])

                        csv1_mapping_dict[each_key].pop(0)
                        csv2_mapping_dict[each_key].pop(0)

                        a_post_val.append(csv1_mapping_dict[each_key])
                        b_post_val.append(csv2_mapping_dict[each_key])
                        lower_csv1_mapping_val = [x.lower() if type(x) == str else str(x) for x in
                                                  csv1_mapping_dict.get(each_key)]
                        lower_csv2_mapping_val = [x.lower() if type(x) == str else str(x) for x in
                                                  csv2_mapping_dict.get(each_key)]
                        if lower_csv1_mapping_val == lower_csv2_mapping_val:
                            common_val.append(csv1_mapping_dict[each_key])

                        else:
                            comment = compare_rows(lower_csv1_mapping_val, lower_csv2_mapping_val, a_post_header)
                            csv1_mapping_dict_key_cp.append(comment)
                            non_matched_vals.append(csv1_mapping_dict_key_cp)

                    elif each_key not in csv2_mapping_dict:
                        csv1_mapping_dict[each_key].pop(0)
                        A_not_B.append(csv1_mapping_dict[each_key])
                    else:
                        pass
                for each_key in csv2_mapping_dict:
                    if each_key not in csv1_mapping_dict:
                        csv2_mapping_dict[each_key].pop(0)
                        B_not_A.append(csv2_mapping_dict[each_key])

                a_post_val_df = pd.DataFrame(a_post_val)

                if not a_post_val_df.empty:
                    a_post_val_r_df_h = a_post_val_df.set_axis(a_post_header, axis=1, inplace=True)
                else:
                    pass

                if not a_post_val_df.empty:
                    a_post_val_df.to_csv("Output/PG/" + first_file + "_POST.csv", index=False, header=a_post_header,
                                         encoding="UTF-8")
                else:
                    with open("Output/PG/" + first_file + "_POST.csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(title_table_1)

                b_post_val_df = pd.DataFrame(b_post_val)

                if not b_post_val_df.empty:
                    b_post_val_r_df_h = b_post_val_df.set_axis(b_post_header, axis=1, inplace=True)
                else:
                    pass

                if not b_post_val_df.empty:
                    b_post_val_df.to_csv("Output/ORA/" + second_file + "_POST.csv", index=False, header=b_post_header,
                                         encoding="UTF-8", errors='ignore')
                else:
                    with open("Output/ORA/" + second_file + "_POST.csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(title_table_2)

                common_val_df = pd.DataFrame(common_val)

                if not common_val_df.empty:
                    common_val_r_df_h = common_val_df.set_axis(a_post_header, axis=1, inplace=True)
                if not common_val_df.empty:
                    common_val_df.to_csv("Output/" + "matched_records" + first_file + second_file + ".csv",
                                         index=False,
                                         header=a_post_header, encoding="UTF-8", errors='ignore')
                else:
                    with open("Output/" + "matched_records" + first_file + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_post_header)

                non_matched_vals_df = pd.DataFrame(non_matched_vals)
                a_vs_b_header = list(b_post_header)
                a_vs_b_header.append("Comment")
                a_vs_b_header.insert(0, "index")

                if not non_matched_vals_df.empty:
                    non_matched_val_r_df_h = non_matched_vals_df.set_axis(a_vs_b_header, axis=1, inplace=True)

                if not non_matched_vals_df.empty:
                    non_matched_vals_df.to_csv("Output/" + first_file + "_VS_" + second_file + ".csv", index=False,
                                               header=a_vs_b_header, encoding="UTF-8", errors='ignore')
                else:
                    with open("Output/" + first_file + "_VS_" + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_vs_b_header)

                a_notin_b_vals_df = pd.DataFrame(A_not_B)

                if not a_notin_b_vals_df.empty:
                    a_notin_b_val_r_df_h = a_notin_b_vals_df.set_axis(a_post_header, axis=1, inplace=True)

                if not a_notin_b_vals_df.empty:
                    a_notin_b_vals_df.to_csv("Output/PG/" + first_file + "_NOT IN_" + second_file + ".csv",
                                             index=False, header=a_post_header, encoding="UTF-8", errors='ignore')
                else:
                    with open("Output/PG/" + first_file + "_NOT IN_" + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_post_header)

                b_notin_a_vals_df = pd.DataFrame(B_not_A)

                if not b_notin_a_vals_df.empty:
                    b_notin_a_val_r_df_h = b_notin_a_vals_df.set_axis(b_post_header, axis=1, inplace=True)

                if not b_notin_a_vals_df.empty:
                    b_notin_a_vals_df.to_csv("Output/ORA/" + second_file + "_NOT IN_" + first_file + ".csv",
                                             index=False, header=b_post_header, encoding="UTF-8", errors='ignore')
                else:
                    with open("Output/ORA/" + second_file + "_NOT IN_" + first_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(b_post_header)

                file_name = first_file + " VS " + second_file
                eom_input_reader_lines = len(df_1)
                mx_input_reader_lines = len(df_2)
                eom_post_reader_lines = len(a_post_val_df)
                mx_post_reader_lines = len(b_post_val_df)
                not_in_mx_reader_lines = len(a_notin_b_vals_df)
                not_in_eom_reader_lines = len(b_notin_a_vals_df)
                eom_vs_mx_reader_lines = len(non_matched_vals_df)

                not_in_first = not_in_eom_reader_lines / mx_input_reader_lines * 100 if mx_input_reader_lines > 0 else "0"
                not_in_second = not_in_mx_reader_lines / eom_input_reader_lines * 100 if not_in_eom_reader_lines > 0 else "0"
                diff = eom_vs_mx_reader_lines / mx_post_reader_lines * 100 if mx_post_reader_lines > 0 else "0"
                summery_header = ['File Names', 'First File', 'Second File', 'Second File Post Not in First File Post',
                                  '% Not in First Input File', 'First File Post Not in Second File Post',
                                  '% Not in Second Input File',
                                  'First File Post', 'Second File Post', 'Difference', '% Difference', 'Comment']
                summery_result = [file_name, eom_input_reader_lines, mx_input_reader_lines,
                                  not_in_eom_reader_lines, not_in_first, not_in_mx_reader_lines,
                                  not_in_second, eom_post_reader_lines, mx_post_reader_lines, eom_vs_mx_reader_lines,
                                  diff,""]
                output_file.append(summery_result)

                if len(table_not_exists) != 0:
                    for (i, j) in zip(table_not_exists, table_not_exists_comment):
                        not_found_table = [i, "", "", "", "", "", "", "", "", "", "", j]
                        output_file.append(not_found_table)
                    output_file_2 = pd.DataFrame(output_file)
                else:
                    output_file_2 = pd.DataFrame(output_file)

                if flag == "db":
                    non_matched_vals_df = non_matched_vals_df.astype(str)
                    a_notin_b_vals_df = a_notin_b_vals_df.astype(str)
                    b_notin_a_vals_df = b_notin_a_vals_df.astype(str)
                    non_matched_vals_df = _handlenullvalue(non_matched_vals_df)
                    a_notin_b_vals_df = _handlenullvalue(a_notin_b_vals_df)
                    b_notin_a_vals_df = _handlenullvalue(b_notin_a_vals_df)
                    drop_tables_if_exists(first_file, second_file)
                    create_tables_db(engine, first_file, second_file, a_post_val_df, b_post_val_df, common_val_df,
                                     non_matched_vals_df, a_notin_b_vals_df, b_notin_a_vals_df)

                output_file_2.to_csv("SUMMARY_PG_TO_ORA.csv", index=False, header=summery_header, encoding="UTF-8", errors='ignore')
                summery_file = pd.read_csv("SUMMARY_PG_TO_ORA.csv")
                summery_df = pd.DataFrame(summery_file)
                if not summery_df.empty:
                    create_summery(summery_df)

            else:
                print("Comparision not required for this {} and {} tables".format(first_file, second_file))
        except Exception as e:
            print("-------------------Execution Stopped----------------------{}  vs {} ".format(first_file, second_file))
            print(e)
            # traceback.print_exc()
            continue

    # create_summery(table_not_exists, table_not_exists_comment, output_file)
    e_time = time.ctime()
    print(e_time)
    print("Comparison Successful")
    print(time.ctime())
comparision()
