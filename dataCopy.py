import cx_Oracle
import sqlite3


def createTable(crt_stmt, drop_stmt):
    """
    Create table using the statements given in crt_stmt. Drop table if necessary before creation
    If statements are syntactically wrong, the program will terminate.
    :param crt_stmt: DDL statement for creating a table in SQLite
    :param drop_stmt: DDL statement for dropping a table in SQLite
    """
    SQLcur.execute(drop_stmt)
    SQLcur.execute(crt_stmt)


def readData(sel_stmt):
    """
    Reads data from sel_stmt and returns the data records in a list
    :param sel_stmt: Syntactically correct query for Oracle database
    :return: Returns the data in a list
    """
    ORAcur.execute(sel_stmt)
    r = ORAcur.fetchall()
    return r


def insertData(ins_stmt, ins_rows):
    """
    Uses the Insert statement in ins_stmt to insert data available in the list names ins_rows to bulk insert into the table
    :param ins_stmt: Syntactically correct insert statement for SQLite database
    :param ins_rows: Data in a list that is formatted to the structure of the table and insert statement parameters
    """
    SQLcur.executemany(ins_stmt, ins_rows)


if __name__ == "__main__":

    print("Start")

    # 1. Establish conenction to Oracle database
    ORAcon = cx_Oracle.connect(
        user="Alex",
        password="alex",
        dsn="10.0.0.123/ORCL"
    )
    ORAcur = ORAcon.cursor()

    # 2. Establish connection to SQLite database
    SQLcon = sqlite3.connect(r"C:\Users\athur\sqlitedata\test.db")
    SQLcur = SQLcon.cursor()

    # 3. Create select queries for Oracle table
    ORAsel = [
        "Select * from HR.countries",
        "Select * from HR.departments",
        "Select * from HR.employees",
        "Select * from HR.jobs",
        "Select * from HR.job_history",
        "Select * from HR.locations",
        "Select * from HR.regions"
    ]

    # 4. Create drop statements for the SQLite database
    droptbls = [
        "drop table if exists countries",
        "drop table if exists departments",
        "drop table if exists employees",
        "drop table if exists jobs",
        "drop table if exists job_history",
        "drop table if exists locations",
        "drop table if exists regions"
    ]

    # 5. Create statements for SQLite database
    createtbls = [
        """CREATE TABLE COUNTRIES (COUNTRY_ID text, COUNTRY_NAME text, REGION_ID numeric)""",
        """CREATE TABLE DEPARTMENTS (DEPARTMENT_ID numeric,DEPARTMENT_NAME text, MANAGER_ID numeric,
        LOCATION_ID numeric)""",
        """CREATE TABLE EMPLOYEES (EMPLOYEE_ID numeric,FIRST_NAME text,LAST_NAME text,
        EMAIL text,PHONE_NUMBER text,HIRE_DATE text,JOB_ID text,SALARY numeric,
        COMMISSION_PCT numeric,MANAGER_ID numeric, DEPARTMENT_ID numeric)""",
        """CREATE TABLE JOBS (JOB_ID text,JOB_TITLE numeric,
        MIN_SALARY numeric,MAX_SALARY numeric)""",
        """CREATE TABLE JOB_HISTORY (EMPLOYEE_ID numeric,START_DATE text,
        END_DATE text,JOB_ID text,DEPARTMENT_ID numeric)""",
        """CREATE TABLE LOCATIONS (LOCATION_ID numeric,STREET_ADDRESS text,
        POSTAL_CODE text,CITY text,STATE_PROVINCE text,COUNTRY_ID text)""",
        """CREATE TABLE REGIONS (REGION_ID numeric, REGION_NAME text)"""
        ]

    # 6. Insert statements for SQLite database
    SQLins = [
        "Insert into countries values (?, ?, ?)",
        "Insert into departments values (?, ?, ?, ?)",
        "Insert into employees values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "Insert into jobs values (?, ?, ?, ?)",
        "Insert into job_history values (?, ?, ?, ?, ?)",
        "Insert into locations values (?, ?, ?, ?, ?, ?)",
        "Insert into regions values (?, ?)",
    ]
    # 7. Go through the list in order and then create SQLite table, fetch data from oracle, then insert into SQLite
    for i in range(0, len(ORAsel)):
        print("processing data for table " + ORAsel[i])
        createTable(createtbls[i], droptbls[i])
        rows = readData(ORAsel[i])
        insertData(SQLins[i], rows)

    # 8. Commit changes and then close connections
    ORAcon.commit()
    ORAcon.close()
    SQLcon.commit()
    SQLcon.close()
