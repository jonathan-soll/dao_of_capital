from simfin import SimFin_DataRequestor
import sql2
import sys
import numpy as np

def generate_random_tickers(schema, table, num_tickers = 5, sqlconnector = sql2.SQLConnect()):
    """
    Generate and return a random list of SimFin tickers that are not in the given SQL table
    """
    assert (table == 'balancesheet' or table == 'incomestatement'), "Table must be 'balancesheet' or 'incomestatement'"

    querystr = """SELECT t.ticker
                    FROM SimFin.all_tickers t WITH (NOLOCK)
                    LEFT JOIN %s.%s t2 WITH (NOLOCK) ON t.simId = t2.simId
                    WHERE
                        t2.simId IS NULL
                        """ % (schema, table)
    result = sqlconnector.query_db(querystr)
    tickers = np.random.choice([x[0] for x in result], num_tickers, replace = False)
    return list(tickers)

if __name__ == '__main__':

    # handle command-line arguments
    API_KEY = sys.argv[1] if len(sys.argv) > 1 else 'N8XsZtAZtjMH8aUVK0d4A6HXm152V0TF'

    my_data_requestor = SimFin_DataRequestor(API_KEY=API_KEY)
    schema = 'SimFin'
    table = 'balancesheet'
    tickers = generate_random_tickers(schema, table)
    print(tickers)
