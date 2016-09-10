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
	path = "/data"

	for datafile in datafiles:
		print datafile
		# find table data belongs in
		# add data to table


def connect_to_db():
	try:
		conn = psycopg2.connect("dbname='clover' host='localhost'")
		print "connected to db"
		return conn
	except:
		print "I am unable to connect to the database"


if __name__=="__main__":

	conn = connect_to_db()

	if conn:
		# specfiles = os.listdir("specs")
		# load_specs(specfiles)
		# print "created tables"

		datafiles = os.listdir("data")
		load_data(datafiles)
		print "loaded data"
		
		conn.close()