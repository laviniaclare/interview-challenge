import unittest
import sys
from seed import connect_to_db, create_table


class Tests(unittest.TestCase):
    """Tests for my party site."""

    def setUp(self):
        """Connect to testing db"""
        self.conn = connect_to_db("testing")  

    def tearDown(self):
        try:
            cur.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
            rows = cur.fetchall()
            for row in rows:
                print "dropping table: ", row[1]
                cur.execute("drop table " + row[1] + " cascade")
            cur.close()
        except:
            print "Error: ", sys.exc_info()[1]
        self.conn.close()

    def test_create_table(self):
        table_name = "people"
        table_schema = [("firstname", "10", "VARCHAR"), ("lastname", "10", "VARCHAR"), ("age", "3", "INTEGER"), ("active", "1", "BOOLEAN")]
        cur = self.conn.cursor()
        create_table(table_name, table_schema)

if __name__ == "__main__":
    unittest.main()