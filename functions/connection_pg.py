import psycopg2

def connection_pg(localhost,db_name,user_name,pswd):
    db_connection_1 = psycopg2.connect(host=localhost, database=db_name, user=user_name, password=pswd, port='5432')
    db_cursor_1 = db_connection_1.cursor()
    # print("Success Fully Connected To PG")
    return db_cursor_1