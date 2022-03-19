import cx_Oracle
import sqlite3

if __name__ == "__main__":
    connection = cx_Oracle.connect(
        user="Alex",
        password="alex",
        dsn="10.0.0.123/ORCL"
    )

    print("Succesfully connected to Oracle Database")

    cursor = connection.cursor()
    SQLcon = sqlite3.connect(r"C:\Users\athur\sqlitedata\test.db")
    SQLcur = SQLcon.cursor()
    # Create a table

    cursor.execute("""  
    drop table todoitem
            """)

    cursor.execute("""
        create table todoitem (
            id number generated always as identity,
            description varchar2(4000),
            creation_ts timestamp with time zone default current_timestamp,
            done number(1,0),
            primary key (id))""")

    # Insert some data

    rows = [("Task 1", 0),
            ("Task 2", 0),
            ("Task 3", 1),
            ("Task 4", 0),
            ("Task 5", 1)]

    cursor.executemany("insert into todoitem (description, done) values(:1, :2)", rows)
    print(cursor.rowcount, "Rows Inserted")

    connection.commit()

    # Now query the rows back
    for row in cursor.execute('select description, done from todoitem'):
        if (row[1]):
            print(row[0], "is done")
        else:
            print(row[0], "is NOT done")

    cursor.execute("SELECT * FROM todoitem")

    rows = []
    for id, desc, date, done in cursor:
        rows.append((id, desc, date, done))

    print(rows)

    SQLcur.execute("""
    drop table if exists todoitem
    """)

    SQLcur.execute("""
    create table todoitem (
        id numeric,
        description text,
        creation_ts text,
        done numeric,
        primary key (id))""")

    SQLcur.executemany("INSERT INTO todoitem VALUES (?, ?, ?, ?)", rows)

    SQLcon.commit()
    SQLcon.close()