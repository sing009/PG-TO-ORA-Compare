import warnings
warnings.filterwarnings("ignore")


class Oracle_load():
    def __init__(self, _credentials):
        #oracle connection
        import cx_Oracle
        from sqlalchemy import types, create_engine
        print("Connecting database.....")
        try:
            self.engine = create_engine('oracle://'+_credentials["user"]+':'+_credentials["password"]+'@'+_credentials["host"]+':'+str(_credentials["port"])+'/'+_credentials["db"]+'')
            dsn_tns = cx_Oracle.makedsn(_credentials["host"], str(_credentials["port"]),service_name=_credentials["db"])
            self.con = cx_Oracle.connect(user=_credentials["user"], password=_credentials["password"], dsn=dsn_tns)
        except Exception as e:
            print(e)
            exit()
        finally:
            print("Connection Extablished..")

    #CONVERT TO ORACLE DATATYPE
    def _cnvt_df_or_dtype(self, dtype, mlen):
        if dtype == 'bool':
            return 'NUMBER(1)'
        elif dtype == 'int64':
            if mlen == 1:
                return 'NUMBER(1)'
            elif mlen <= 8:
                return 'NUMBER(8,0)'
            else:
                return 'NUMBER(38,0)'
        elif dtype == 'float64':
            return 'NUMBER(' + str(mlen + 2) + ',2)'
        elif dtype == 'datetime64':
            return 'TIMESTAMP'
        elif dtype == 'object':
            return 'VARCHAR2(' + str(mlen) + ')'
        else:
            return 'VARCHAR2(255)'

    #check table exists
    def _check_table_exists(self, sTableName):
        with self.con.cursor() as cur:
            _res = cur.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = '"+sTableName+"'")
            _cnt=_res.fetchall()
            if _cnt[0][0] > 0:
                return 1
        return 0 #default false

    def _create_table_from_df(self, df, sTableName):
        import pandas as pd
        import numpy as np
        print("Fetching csv attribute...")
        df=df.head(1000)
        measure = np.vectorize(len)
        col_attr = list(zip(df.dtypes.iteritems(),measure(df.values.astype(str)).max(axis=0)))
        print("create statement.....")
        xtxt=''
        for i in col_attr:
            if xtxt != '':
                xtxt+=', '
            xtxt += '' + i[0][0] + ' ' + self._cnvt_df_or_dtype(i[0][1], i[1])

        _sql = 'CREATE TABLE ' + sTableName + '(' + xtxt + ');'
        print(_sql)
        # ------------------------------------------------
        print('Execute Query....')
        try:
            with self.con.cursor() as cur:
                cur.execute(_sql)
                self.con.commit()
                print("con.commit()")
        except Exception as e:
            print(e)
        finally:
            return 0

        return 1

    def _load_data(self, sTableName, df):
        try:
            #connection= self.engine.raw_connection()
            df.to_sql(sTableName, self.engine, if_exists='replace')
        except Exception as e:
            print(e)
        finally:
            print(sTableName)
            print("load complete..")
            return 0
        return 1 #default false

    def _load_data_from_csv_auto(self, csvFile, sTableName):
        import pandas as pd
        #read csv to df
        df = pd.read_csv(csvFile,encoding = "ISO-8859-1")
        if df.shape[0] ==0:
            print("Error: "+csvFile+" is empty...")
        else:
            if self._check_table_exists(sTableName) == 0:
                self._create_table_from_df(df, sTableName)
            #spliting df
            if df.shape[0] > 10000:
                cnk_no= int(df.shape[0] / 10000) + 1
                import numpy as np
                df2=np.array_split(df, cnk_no)
                df=None
                for i in range(cnk_no):
                    print(df2[i].shape)
                    self._load_data(sTableName,df2[i])
            else:
                self._load_data(sTableName,df)
        return 0


if __name__ == "__main__":
    _ip_file = r'./input_directory_csv/Main.csv'
    _op_dir = r'Output'
    import pandas as pd
    _ipdf = pd.read_csv(_ip_file)
    #_compare_files = [('_NOT_IN_'.join(x), x[0].split('.')[1]+'_NO_IN_'+x[1].split('.')[1]) for x in _ipdf[(_ipdf.COMPARE_NEEDED == 'YES')][["ORA_TABLE_NAME","PG_TABLE_NAME"]].values]
    #_compare_files = [('_VS_'.join(x), x[0].split('.')[1]+'_VS_'+x[1].split('.')[1]) for x in _ipdf[(_ipdf.COMPARE_NEEDED == 'YES')][["ORA_TABLE_NAME","PG_TABLE_NAME"]].values]
    _pg_vs_ora = [ x[0].split('.')[1]+'_VS_'+x[1].split('.')[1] for x in _ipdf[(_ipdf.COMPARE_NEEDED == 'YES')][["ORA_TABLE_NAME","PG_TABLE_NAME"]].values]
    _pg_not_in_ora = ['ORA_'+x[0].split('.')[1]+'_NOT_IN_PG_'+x[1].split('.')[1] for x in _ipdf[(_ipdf.COMPARE_NEEDED == 'YES')][["ORA_TABLE_NAME","PG_TABLE_NAME"]].values]
    _ora_not_in_pg = ['PG_'+x[0].split('.')[1]+'_NOT_IN_ORA_'+x[1].split('.')[1] for x in _ipdf[(_ipdf.COMPARE_NEEDED == 'YES')][["ORA_TABLE_NAME","PG_TABLE_NAME"]].values]

    _compare_files = _pg_vs_ora + _pg_not_in_ora + _ora_not_in_pg
    print(_compare_files)
    #check file exists
    import os
    _list_file_found=[]
    for file in _compare_files:
        file_path=_op_dir+'/'+file+'.CSV'
        print(file_path)
        if os.path.exists(file_path):
            _list_file_found.append(file)

    #Oracle_load onject
    # _obj = Oracle_load({"host":"localhost", "port":9480, "user":"testuser", "password": "testuser", "db":"oratest"})
    _obj = Oracle_load({"host": "ora-tst.cgin5nplrvzw.eu-central-1.rds.amazonaws.com", "port": 1480, "user": "testuser", "password": "testuser", "db": "oratest"})

    print(_list_file_found)
    for file in _list_file_found:
        file_path = _op_dir+'/'+file+'.CSV'
        _obj._load_data_from_csv_auto(file_path,sTableName=file)
