import psycopg2
import os


def populate_db(path="specs/"):
    """"Populates database using files in specs folder and data folder"""
    specfiles = os.listdir("specs")

    for specfile in specfiles:
        #remove '.csv' from end to get table name
        table_name = specfile[:-4]
        #open file with table specs representing table schema
        table_specs = open(path+specfile)
        # create schema to be used by create table function and load data function
        table_schema = create_table_schema(table_specs)
        # create the table in db
        create_table(table_name, table_schema)
        # load data into table
        load_table_data(table_name, table_schema)


def create_table_schema(specfile):
    """takes in a spec file for a table, returns list of tuples representing schema for table"""
    schema = []

    # looping through lines in specfile, skipping the header line
    for line in specfile.readlines()[1:]:

        column = line.strip().split(",")
        column_name = column[0]
        column_width = column[1]
        column_type = column[2]

        if column_type == 'TEXT':
            # postgres does not recognize type "TEXT", so I chose to change it to "VARCHAR", which is meant
            # to represent strings of varying length and will require us to specify a max length.
            column_type = 'VARCHAR'

        schema.append((column_name, column_width, column_type))

    return schema


def create_table(table_name, schema):
    """takes in name and schema then creates table in db fitting specifications"""

    print "\n\nCREATING TABLE ", table_name

    # initalizing query string
    query = """CREATE TABLE %s (""" % table_name

    # looping through columns in schema to construct full CREATE TABLE query
    for column in schema:
        if column[2] == 'VARCHAR':
            # varchar takes in an argument max length, so if our column is that type we need to provide it.
            column_string = '%s %s(%s), ' % (column[0], column[2], column[1])
        else:
            # booleans and integers do not take in any parameters.
            column_string = '%s %s, ' % (column[0], column[2])

        query += column_string

    #remove extra space and comma from last column
    query = query[:-2]
    query += ');'

    # execute query and commit changes
    cur.execute(query)
    conn.commit()


def load_table_data(table_name, table_schema, path="data/"):
    """Takes in a table name, schema, and optional path parameter and
    inserts data from data files into table"""

    print "\n\nLOADING DATA INTO TABLE ", table_name

    # get list of file objects with data to be added to given table.
    table_data_files = [open(path+file_name) for file_name in os.listdir(path[:-1]) if file_name.startswith(table_name)]

    # NOTE: Does not take into account data added. if this function were run periodically as data files were added,
    # taking date into account would be necessary to avoid duplicate data entry.

    # each data file may contain multiple rows of data so we loop through each file, and each row in that file
    for data_file in table_data_files:
        data = data_file.readlines()
        # insert data one row at a time. Is there a more efficient way to do this?
        for row in data:
            insert_row_into_table(row, table_name, table_schema)


def insert_row_into_table(row, table_name, table_schema):
    """Takes in row (represented by a string), table name (string), and table_schema
    (list of tuples) and inserts data from row into the table."""

    print "\nLOADING ROW", row

    # creating start of query strings to be added to
    insert_into = """INSERT INTO %s (""" % table_name
    values = """ VALUES ("""

    # Looping through each column in the schema to add column name and value to query strings
    for column in table_schema:
        colum_name = column[0]
        column_width = int(column[1])
        column_type = column[2]
        column_value = row[:column_width].strip()

        # If column type is boolean want to store True or False in the db, but bools are represented as 1 or 0 in data
        # text files so we much convert them to True or False values before inserting into db
        if column_type == "BOOLEAN":
            # I'm assumeing here that booleans will always be represented by 1 or 0 in data text files.
            # if this is not the case the conversion from string to int could throw an error.
            column_value = bool(int(column_value))

        insert_into += "%s, " % colum_name

        # if the column is a varchar type we need to put single quotes around the value, otherwise not quotes needed.
        if column_type == "VARCHAR":
            values += "'%s', " % column_value
        else:
            values += "%s, " % column_value

        row = row[column_width:]

    # Remove extra comma and space from end. I feel like there should be a more elegant way to deal with this.
    insert_into = insert_into[:-2]
    insert_into += ')'
    # Remove extra comma and space from end
    values = values[:-2]
    values += ');'

    # combine insert into and values to get full query
    full_query = insert_into + values

    # execute query and commit changes
    cur.execute(full_query)
    conn.commit()


def connect_to_db(db_name='clover'):
    """Connects to postgres database"""
    try:
        conn = psycopg2.connect("dbname='%s' host='localhost'" % db_name)
        print "connected to db"
        return conn
    except:
        print "I am unable to connect to the database"


if __name__ == "__main__":

    conn = connect_to_db()

    if conn:
        cur = conn.cursor()
        populate_db()
        cur.close()
        conn.close()
