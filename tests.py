import unittest
import sys
import seed


class Tests(unittest.TestCase):
    """Tests for my party site."""

    def setUp(self):
        """Connect to testing db"""
        self.conn = seed.connect_to_db("testing")
        self.cur = self.conn.cursor()
        

    def tearDown(self):
        try:
            self.cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name""")
            tables = self.cur.fetchall()

            for table in tables:
                print "dropping table: ", table[0]
                query = """DROP TABLE %s;""" % table[0]
                self.cur.execute(query)
            self.cur.close()
        except:
            print "Error: ", sys.exc_info()[1]

        self.conn.commit()
        self.conn.close()
        # not sure if line below is needed, but better safe than sorry
        seed.conn.close()

    
    def test_create_table(self):
        # making fake table name and schema
        table_name = "people"
        table_schema = [("firstname", "10", "VARCHAR"), ("lastname", "10", "VARCHAR"), ("age", "3", "INTEGER"), ("active", "1", "BOOLEAN")]
        
        # setting cur and conn in seed to test cur and conn (if we don't do this we get a 'cur is undefined' error)
        seed.cur = self.conn.cursor()
        seed.conn = self.conn
        
        # running function with fake table name and schema
        seed.create_table(table_name, table_schema)

        # testing that table has been created and column names are as expected
        self.cur.execute("""SELECT * FROM people LIMIT 0""")
        colnames = [desc[0] for desc in self.cur.description]
        
        self.assertEqual(colnames, ['firstname', 'lastname', 'age', 'active'])


if __name__ == "__main__":
    unittest.main()
