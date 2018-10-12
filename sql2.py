import pyodbc

class SQLConnect():

    def __init__(self):
        """
        Initializes the class with the variables needed to connect to the database
        """
        self.server = 'firstserverjs.database.windows.net'
        self.database = 'securities_master'
        self.username = 'JSoll'
        self.password = 'Emjosa139'
        self.driver= '{ODBC Driver 17 for SQL Server}'
        self.conn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1433;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        self.curs = self.conn.cursor()

    def reset_cursor(self):
        self.curs.close()
        self.conn.close()
        self.conn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1433;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        self.curs = self.conn.cursor()

    def _df_to_tuples(self, df):
        """
        Converts the fiscal_year and simId to integer objects so it can be properly
        inserted into SQL

        TODO: GENERALIZE THIS MORE
        """
        df = df.fillna(0)
        return [(
                    str(x[0]), int(x[1]), int(x[2]), x[3], int(x[4]), int(x[5]),
                    int(x[6] or 0), int(x[7] or 0), int(x[8] or 0),
                    x[9], int(x[10]), int(x[11]), x[12], x[13]
                ) for x in df.to_records(index=False)]

    def query_db(self, querystr):
        """
        Queries the database and returns a list of tuples
        """
        self.curs.execute(querystr)
        return self.curs.fetchall()

    def truncate_insert_merge(self, schema, stage_table, proc, df):
        """
        This method is used to insert data (df) into a stage table in a SQL database which is then
        merged into a final table via a stored proc. It is assumed that the stage table, final table,
        and stored proc are all in the same schema.
        """

        truncate_str = "TRUNCATE TABLE %s.%s" % (schema, stage_table)

        column_str = ', '.join([x for x in df.columns])
        values_str = ('?, ' * len(df.columns))[:-2]
        insert_str = """INSERT INTO %s.%s (%s)
                        VALUES (%s)""" % (schema, stage_table, column_str, values_str)

        proc_str = "{CALL %s.%s}" % (schema, proc)

        df_tuples = self._df_to_tuples(df)      # convert dataframe to list of tuples

        try:
            self.curs.execute(truncate_str)
            print("TRUNCATE SUCCESSFUL")
        except:
            print("TRUNCATE FAILED")

        self.curs.fast_executemany = True
        try:
            self.curs.executemany(insert_str, df_tuples)
            print("DUMP INTO STAGE SUCCESSFUL")
        except:
            print("DUMP INTO STAGE FAILED")
        try:
            self.curs.execute(proc_str)
            print("MERGE STORED PROC SUCCESSFUL")
        except:
            print("MERGE STORED PROC FAILED")
        self.curs.commit()
