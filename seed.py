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
	path = "specs/"
	for specfile in specfiles:
		#remove '.csv' from end to get table name and change first letter to uppercase
		table_name = specfile[:-4].title()
		#open file with table specs
		table_specs = open(path+specfile)

		query = """CREATE TABLE %s (""" % table_name

		for line in table_specs.readlines()[1:]:
			column = line.split(",")
			print column
			column_name = column[0]
			print column_name
			#create table in db with specs

		query += ");"
		print query


def load_data(datafiles):
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
		specfiles = os.listdir("specs")
		load_specs(specfiles)

		# datafiles = os.listdir("data")
		# load_data(datafiles)
		
		conn.close()