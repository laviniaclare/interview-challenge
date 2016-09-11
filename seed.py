import psycopg2
import os

############## REFERENCES TO DELETE #################
"""CREATE TABLE table_name
(
column_name1 data_type(size),
column_name2 data_type(size),
column_name3 data_type(size),
....
);"""


"""CREATE TABLE Persons
(
PersonID int,
LastName varchar(255),
FirstName varchar(255),
Address varchar(255),
City varchar(255)
);"""

########################################################

def populate_db():
	specfiles = os.listdir("specs")
	
	for specfile in specfiles:
		#remove '.csv' from end to get table name
		table_name = specfile[:-4]
		#open file with table specs
		table_specs = open('specs/'+specfile)		
		# create schema to be used by create table function and load data function
		table_schema = create_table_schema(table_specs)
		print table_schema
		# create the table
		create_table(table_name, table_schema)
		# load data into table
		load_table_data(table_name, table_schema)



def create_table_schema(specfile):
	"""takes in a spec file for a table, returns list of tuples representing schema for table"""
	schema = []

	for line in specfile.readlines()[1:]:
		column = line.strip().split(",")
		column_name = column[0]
		column_width = column[1]
		column_type = column[2]

		if column_type == 'TEXT':
			column_type = 'VARCHAR'

		schema.append((column_name, column_width, column_type))

	return schema


def create_table(table_name, schema):
	"""takes in name, schema, creates table in db"""

	query = """CREATE TABLE %s (""" % table_name

	for column in schema:
		# TODO: Add check for all types that take len args perhaps with set containing column type names.
		# can then check if column type in "column_type_takes_args"
		if column[2] == 'VARCHAR':
			column_string = '%s %s(%s), ' % (column[0], column[2], column[1])
		else:
			column_string = '%s %s, ' % (column[0], column[2])

		query += column_string

	#remove extra space and comma from last column
	query = query[:-2]
	query += ');'
	print query
	cur = conn.cursor()
	cur.execute(query)
	conn.commit()
	cur.close()



def load_table_data(table_name, table_schema):
	"""Takes in a table name and schema and inserts data from data files into table"""
	
	# get list of file objects with data to be added to given table
	table_data_files = [open("data/"+file_name) for file_name in os.listdir("data") if file_name.startswith(table_name)]

	print table_data_files

	for data_file in table_data_files:
		data = data_file.readlines()
		for row in data:
			insert_row_into_table(row, table_name, table_schema)


def insert_row_into_table(row, table_name, table_schema):

	query 
	for column in table_schema:
		colum_name = table_schema[0]
		column_width = table_schema[1]





def load_specs(specfiles):
	path = 'specs/'
	for specfile in specfiles:
		#remove '.csv' from end to get table name
		table_name = specfile[:-4]
		#open file with table specs
		table_specs = open(path+specfile)

		query = """CREATE TABLE %s (""" % table_name

		for line in table_specs.readlines()[1:]:
			column = line.strip().split(",")
			column_name = column[0]
			column_width = column[1]
			column_type = column[2]


			if column_type == 'TEXT':
				column_type = 'VARCHAR'

			# TODO: Add check for all types that take len args perhaps with set containing column type names.
			# can then check if column type in "column_type_takes_args"
			if column_type == 'VARCHAR':
				column_string = '%s %s(%s), ' % (column_name, column_type, column_width)
			else:
				column_string = '%s %s, ' % (column_name, column_type)

			query += column_string

		#remove extra space and comma from last column
		query = query[:-2]
		query += ');'

		cur = conn.cursor()
		cur.execute(query)
		conn.commit()
		cur.close()


def load_data(datafiles):
	path = "data/"

	for datafile in datafiles:
		print datafile
		data = open(path+datafile)
		# find table data belongs in
		table_name = datafile.split('_')[0]
		print table_name

		for row in data.readlines():
			print row
			print row[-5:].strip()


def connect_to_db(db_name = 'clover'):
	"""Connects to postgres database"""
	try:
		conn = psycopg2.connect("dbname='%s' host='localhost'" % db_name)
		print "connected to db"
		return conn
	except:
		print "I am unable to connect to the database"


if __name__=="__main__":

	conn = connect_to_db()

	if conn:

		populate_db()

		# specfiles = os.listdir("specs")
		# load_specs(specfiles)
		# print "created tables"

		# datafiles = os.listdir("data")
		# load_data(datafiles)
		# print "loaded data"
		
		conn.close()