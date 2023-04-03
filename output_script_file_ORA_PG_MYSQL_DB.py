import copy
import os.path
import csv
import traceback
import pandas as pd
import time
import mysql.connector
import re
from sqlalchemy import create_engine

#------------------------------------------------------------------------
# path = "D:\\Python_sample_my\\update_table_table compare\\ORA_PG\\"

#------------------------------------------------------------------------


# input excel file path
inputExcelFile = os.getcwd() + "\\input_directory_xl\\Compare_Template_updated_1_file_sorted_for_no_PK.xlsx"

# Reading an excel file
excelFile = pd.read_excel(inputExcelFile, sheet_name=None)

# Converting excel file into CSV file
for key in excelFile.keys():
    excelFile[key].to_csv("input_directory_csv/" + ("%s.csv" % key), index=None, header=True)

csv_file_names = list(excelFile.keys())


first_db = 'comapare'
second_db = 'compare_dff'
user_name = 'root'
localhost = 'localhost'
pswd = 'root'
flag = "local"

input_main_csv_path = "input_directory_csv/Main.csv"
input_keys_csv_path = "input_directory_csv/Keys.csv"
input_ignore_COl_csv_path = "input_directory_csv/Ignored Columns.csv"

def fetch_db_1(file):
    db_connection_1 = mysql.connector.connect(host=localhost, database=first_db, user=user_name, password=pswd)
    db_cursor_1 = db_connection_1.cursor()
    db_cursor_1.execute('SELECT * FROM ' + file)
    table_rows_1 = db_cursor_1.fetchall()
    db_title_1 = [i[0] for i in db_cursor_1.description]
    return table_rows_1, db_title_1


def fetch_db_2(file):
    db_connection_2 = mysql.connector.connect(host=localhost, database=second_db, user=user_name, password=pswd)
    db_cursor_2 = db_connection_2.cursor()
    db_cursor_2.execute('SELECT * FROM ' + file)

    table_rows_2 = db_cursor_2.fetchall()
    db_title_2 = [i[0] for i in db_cursor_2.description]
    return table_rows_2, db_title_2


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


def exclude_col(dataframes, exclude_col_data, exclude_col_elements, pk_elements):
    pk_list_val = list(pk_elements)
    # pk and exclude must not be same
    res = [check_val for check_val in exclude_col_elements if check_val in pk_list_val]
    if len(res) >= 1:
        raise Exception("Unable to perform")
    else:
        result_for_ignoring_col = (dataframes.loc[:, dataframes.columns != exclude_col_data])
    return result_for_ignoring_col

def pk_merged_data(index_list,all_headers_table,title_table,csv_data):
    num_convert_header_name = []
    for each_ind in index_list:
        if each_ind.isnumeric() or re.findall(r"[-+]?\d*\.*\d+", each_ind):
            float_convert = float(each_ind)
            ind = int(float_convert)
            num_convert_header_name.append(all_headers_table[ind - 1])
        else:
            num_convert_header_name.append(each_ind)
    pk = ""
    for each_header in num_convert_header_name:

        if True:
            ind = title_table.index(each_header)
            ind = ind + 1
        pk += str(csv_data[ind])

    return pk


def comparision():
    print(time.ctime())
    print("Comparison start ...")
    input_csv = pd.read_csv(input_main_csv_path)

    comparision_check = "COMPARE_NEEDED"
    Primary_KEYS_check = "KEYS_AVAILABLE"
    Exclude_col_check = "COLUMN_NEED_TO_IGNORE"

    # ----------------------------------------------compare_check ----------------------------------------------------------------
    inputCSV_header = list(input_csv.columns)

    if os.path.exists("summary.csv"):
        os.remove("summary.csv")

    pk_set = set()

    def check_required_or_not(check):
        check_res = ""
        find_indx_pos = inputCSV_header.index(check)
        res_val = str(input_csv_data[find_indx_pos]).split(" ")
        check_res += res_val[0]
        return check_res

    for input_csv_data in input_csv.itertuples(index=False):
        try:
            first_file = input_csv_data[0]
            second_file = input_csv_data[1]
            index_list = []
            # ============================================check comparision ===============================
            compare_check_res = ""
            if comparision_check in input_csv:
                compare_check_res += check_required_or_not(comparision_check)

            if compare_check_res.casefold() == "YES".casefold():

                if flag == "db":
                    data_1, title_table_1 = fetch_db_1(first_file)
                    table_df_1 = pd.DataFrame(data_1)
                    data_2, title_table_2 = fetch_db_2(second_file)
                    table_df_2 = pd.DataFrame(data_2)
                elif flag == "local":

                    first_file_path = first_file + ".csv"
                    table_df_1 = pd.read_csv(first_file_path,low_memory=False,encoding='latin-1')
                    title_table_1 = table_df_1.columns.tolist()

                    second_file_path = second_file + ".csv"
                    table_df_2 = pd.read_csv(second_file_path,low_memory=False,encoding='latin-1')
                    title_table_2 = table_df_2.columns.values.tolist()
                else:
                    data_1, title_table_1, table_df_1 = [], [], []
                    data_2, title_table_2, table_df_2 = [], [], []

                all_headers_table_1 = copy.deepcopy(title_table_1)
                all_headers_table_2 = copy.deepcopy(title_table_2)

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

                        input_pk_csv_file = pd.read_csv(input_keys_csv_path)
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

                                if len(index_list) > 0 and '0'  not in index_list and "nan" not in index_list and '0.0'  not in index_list  :

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
                        print("NO PrimaryKeys available , it can Compare  String Based {} and  {}".format(first_file,second_file))

                exclude_col_check_res = ""
                if Exclude_col_check in input_csv:
                    exclude_col_check_res += check_required_or_not(Exclude_col_check)

                    if exclude_col_check_res.casefold() == "YES".casefold():
                        dict_val_exclude = {}

                        input_eclude_csv_file = pd.read_csv(input_ignore_COl_csv_path)
                        for each_exclude_key in input_eclude_csv_file.itertuples(index=False):
                            exclude_col_df = pd.DataFrame(each_exclude_key)
                            if not exclude_col_df.empty:
                                dict_val_exclude[exclude_col_df[0][0]] = (exclude_col_df[0][1:].tolist())

                        for each_val in dict_val_exclude:
                            if first_file == each_val:
                                exclude_names = dict_val_exclude[each_val]
                                exclude_header = [item for item in exclude_names if not (pd.isnull(item)) == True]
                                exclude_col_numbers = []
                                if len(exclude_header) >=1:
                                    exclude_ele_convert_str = []
                                    for convert_str_ele in exclude_header:
                                        if convert_str_ele == 0 or convert_str_ele == 0.0 :
                                            pass
                                        elif type(convert_str_ele) ==float or type(convert_str_ele)==int :
                                            if type(convert_str_ele) ==float:
                                                val_add = int(convert_str_ele)
                                            else:
                                                val_add = int(convert_str_ele)
                                            exclude_ele_convert_str.append(str(val_add))
                                        else:
                                            exclude_ele_convert_str.append(str(convert_str_ele))

                                    for exclude_data_col in exclude_ele_convert_str:
                                        if exclude_data_col.isdigit() :
                                            exclude_col_numbers_str_to_int = int(exclude_data_col)
                                            exclude_col_numbers.append(title_table_1[exclude_col_numbers_str_to_int - 1])
                                            table_df_1 = exclude_col(table_df_1,title_table_1[exclude_col_numbers_str_to_int - 1],exclude_col_numbers, pk_set)
                                            table_df_2 = exclude_col(table_df_2,title_table_2[exclude_col_numbers_str_to_int - 1],exclude_col_numbers, pk_set)
                                        else:

                                            exclude_col_numbers.append(exclude_data_col)
                                            table_df_1 = exclude_col(table_df_1, exclude_data_col, exclude_col_numbers,pk_set)
                                            table_df_2 = exclude_col(table_df_2, exclude_data_col, exclude_col_numbers,pk_set)

                                    df_1 = table_df_1
                                    df_2 = table_df_2

                                    a_post_header.clear()
                                    b_post_header.clear()

                                    header_A = df_1.columns.values
                                    a_post_header.extend(header_A)

                                    header_B = df_2.columns.values
                                    b_post_header.extend(header_B)

                                else:
                                    pass

                df_1.index += 1
                df_2.index += 1

                engine = create_engine('mysql://root:root@localhost/output')
                if flag == "db":
                    mydb = mysql.connector.connect(
                        host="localhost",
                        user="root",
                        password="root",
                        database="output"
                    )
                    mycursor = mydb.cursor()

                    del_first_table_if_exits = "DROP TABLE IF EXISTS eom_post"
                    del_second_table_if_exits = "DROP TABLE IF EXISTS " + second_file + "_post;"
                    del_matched_table_if_exits = "DROP TABLE IF EXISTS " + "matched_records" + first_file + second_file + ";"
                    del_non_matched_table_if_exits = "DROP TABLE IF EXISTS " + first_file + "_vs_" + second_file + ";"
                    del_First_notin_Second_table_if_exits = "DROP TABLE IF EXISTS " + first_file + "_notin_" + second_file + ";"
                    del_Second_notin_First_table_if_exits = "DROP TABLE IF EXISTS " + second_file + "_notin_" + first_file + ";"

                    mycursor.execute(del_first_table_if_exits)
                    mycursor.execute(del_second_table_if_exits)
                    mycursor.execute(del_matched_table_if_exits)
                    mycursor.execute(del_non_matched_table_if_exits)
                    mycursor.execute(del_First_notin_Second_table_if_exits)
                    mycursor.execute(del_Second_notin_First_table_if_exits)

                csv1_mapping_dict = {}
                csv2_mapping_dict = {}

                for csv1_data in df_1.itertuples():

                    if primary_key_check_res.casefold() == "YES".casefold():  # DF_1  ---> pk is available --> Now PKBASED Compare



                        if len(index_list) > 0 and '0' not in index_list and "nan" not in index_list and '0.0' not in index_list:
                            pk_1 = pk_merged_data(index_list,all_headers_table_1,title_table_2,csv1_data)
                            csv1_mapping_dict[pk_1] = list(csv1_data)

                    else:   # --> string_Based Compare

                        ax = []
                        pk=""
                        for each_1 in csv1_data:
                            each_ele_str_1 = str(each_1)
                            ax.append(each_ele_str_1)
                        pk += ",".join(ax[1::])
                        csv1_mapping_dict[pk] = list(csv1_data)



                for csv2_data in df_2.itertuples():
                    if primary_key_check_res.casefold() == "YES".casefold():  # DF_2  ---> pk is available --> Now PKBASED Compare

                        if len(index_list) > 0 and '0' not in index_list and "nan" not in index_list:

                            pk_2 = pk_merged_data(index_list, all_headers_table_2, title_table_2, csv2_data)
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
                    a_post_val_df.to_csv("Output/" + first_file + "_POST.csv", index=False, header=a_post_header,
                                         encoding="UTF-8")
                else:
                    with open("Output/" + first_file + "_POST.csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(title_table_1)

                if flag == "db":

                    if not a_post_val_df.empty:
                        with engine.connect() as conn, conn.begin():
                            a_post_val_df.to_sql(first_file + "_post", conn, if_exists='append', index=False)

                b_post_val_df = pd.DataFrame(b_post_val)

                if not b_post_val_df.empty:
                    b_post_val_r_df_h = b_post_val_df.set_axis(b_post_header, axis=1, inplace=True)
                else:
                    pass

                if not b_post_val_df.empty:
                    b_post_val_df.to_csv("Output/" + second_file + "_POST.csv", index=False, header=b_post_header,
                                         encoding="UTF-8")
                else:
                    with open("Output/" + second_file + "_POST.csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(title_table_2)

                if flag == "db":
                    if not b_post_val_df.empty:
                        with engine.connect() as conn, conn.begin():
                            b_post_val_df.to_sql(second_file + "_post", conn, if_exists='append', index=False)

                common_val_df = pd.DataFrame(common_val)

                if not common_val_df.empty:
                    common_val_r_df_h = common_val_df.set_axis(a_post_header, axis=1, inplace=True)
                if not common_val_df.empty:
                    common_val_df.to_csv("Output/" + "matched_records" + first_file + second_file + ".csv", index=False,
                                         header=a_post_header, encoding="UTF-8")
                else:
                    with open("Output/" + "matched_records" + first_file + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_post_header)

                if flag == "db":
                    if not common_val_df.empty:
                        with engine.connect() as conn, conn.begin():
                            common_val_df.to_sql("matched_records" + first_file + second_file, conn, if_exists='append',
                                                 index=False)

                non_matched_vals_df = pd.DataFrame(non_matched_vals)
                a_vs_b_header = list(b_post_header)
                a_vs_b_header.append("Comment")
                a_vs_b_header.insert(0, "index")

                if not non_matched_vals_df.empty:
                    non_matched_val_r_df_h = non_matched_vals_df.set_axis(a_vs_b_header, axis=1, inplace=True)

                if not non_matched_vals_df.empty:
                    non_matched_vals_df.to_csv("Output/" + first_file + "_VS_" + second_file + ".csv", index=False,
                                               header=a_vs_b_header, encoding="UTF-8")
                else:
                    with open("Output/" + first_file + "_VS_" + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_vs_b_header)

                if flag == "db":

                    if not non_matched_vals_df.empty:
                        with engine.connect() as conn, conn.begin():
                            non_matched_vals_df.to_sql(first_file + "_vs_" + second_file, conn, if_exists='append',
                                                       index=False)

                a_notin_b_vals_df = pd.DataFrame(A_not_B)

                if not a_notin_b_vals_df.empty:
                    a_notin_b_val_r_df_h = a_notin_b_vals_df.set_axis(a_post_header, axis=1, inplace=True)

                if not a_notin_b_vals_df.empty:
                    a_notin_b_vals_df.to_csv("Output/" + first_file + "_NOT IN_" + second_file + ".csv",
                                             index=False, header=a_post_header, encoding="UTF-8")
                else:
                    with open("Output/" + first_file + "_NOT IN_" + second_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(a_post_header)

                if flag == "db":
                    if not a_notin_b_vals_df.empty:
                        with engine.connect() as conn, conn.begin():
                            a_notin_b_vals_df.to_sql(first_file + "_notin_" + second_file, conn, if_exists='append',
                                                     index=False)

                b_notin_a_vals_df = pd.DataFrame(B_not_A)

                if not b_notin_a_vals_df.empty:
                    b_notin_a_val_r_df_h = b_notin_a_vals_df.set_axis(b_post_header, axis=1, inplace=True)

                if not b_notin_a_vals_df.empty:
                    b_notin_a_vals_df.to_csv("Output/" + second_file + "_NOT IN_" + first_file + ".csv",
                                             index=False, header=b_post_header, encoding="UTF-8")
                else:
                    with open("Output/" + second_file + "_NOT IN_" + first_file + ".csv", 'w') as eh:
                        writer = csv.writer(eh)
                        writer.writerow(b_post_header)

                if flag == "db":

                    if not b_notin_a_vals_df.empty:
                        with engine.connect() as conn, conn.begin():
                            b_notin_a_vals_df.to_sql(second_file + "_notin_" + first_file, conn, if_exists='append',
                                                     index=False)

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

                output_file = []
                summery_header = ['File Names', 'First File', 'Second File', 'Second File Post Not in First File Post',
                                  '% Not in First Input File', 'First File Post Not in Second File Post',
                                  '% Not in Second Input File',
                                  'First File Post', 'Second File Post', 'Difference', '% Difference']

                summery_result = [file_name, eom_input_reader_lines, mx_input_reader_lines,
                                  not_in_eom_reader_lines, not_in_first, not_in_mx_reader_lines,
                                  not_in_second, eom_post_reader_lines, mx_post_reader_lines, eom_vs_mx_reader_lines,
                                  diff]
                output_file.append(summery_result)
                output_file_2 = pd.DataFrame(output_file)
                if os.path.exists("summary.csv"):
                    summary_csv = pd.read_csv("summary.csv")
                    output_file_2.to_csv('summary.csv', mode='a', index=False, header=False)

                else:
                    if not output_file_2.empty:
                        output_file_2.to_csv("summary.csv", index=False, header=summery_header)

                e_time = time.ctime()
                print(e_time)
                print("Comparison Successful")
                print(time.ctime())
            else:
                print("Comparision not required for this {} and {} tables".format(first_file, second_file))
        except Exception as e:
            print("-------------------Execution Stopped---------------------- "
                  "\n Primary key and Excludes columns Matched")
            traceback.print_exc()
            continue


comparision()
