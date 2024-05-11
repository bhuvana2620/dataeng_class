import time
import psycopg2
import argparse
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "Bhanu@162019"
StagingTableName = 'censusdata_staging'
MainTableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'', '')  # TIDY: eliminate quotes within literals

    ret = f"""
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
      {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
    """

    return ret

def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable

# Read the input data file into a list of row strings
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        rowlist = [row for row in dr]
    return rowlist

# Convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist, table):
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {table} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist

# Connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = False
    return connection

# Create the staging table if it does not exist
def createStagingTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS {StagingTableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );
        """)

        print(f"Created {StagingTableName} table")

# Load data into the staging table
# Append data from the staging table to the main table
def loadStagingTable(conn, icmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows into \"{StagingTableName}\"")
        start = time.perf_counter()
        for cmd in icmdlist:
            cursor.execute("INSERT INTO %s VALUES (%s)", (StagingTableName, cmd))
        elapsed = time.perf_counter() - start
        print(f'Finished loading into \"{StagingTableName}\". Elapsed Time: {elapsed:0.4} seconds')

# Create index and constraint on the main table
def createIndexAndConstraint(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{MainTableName}_State ON {MainTableName}(State);")
        cursor.execute(f"ALTER TABLE {MainTableName} ADD PRIMARY KEY (CensusTract);")
        print(f"Created index and constraint on {MainTableName}")

def main():
    initialize()
    conn = dbconnect()
    row_list = readdata(Datafile)
    staging_cmd_list = getSQLcmnds(row_list, StagingTableName)

    if CreateDB:
        createStagingTable(conn)

    loadStagingTable(conn, staging_cmd_list)
    appendDataToMainTable(conn)
    createIndexAndConstraint(conn)

if __name__ == "__main__":
    main()