"""
Get daily price data by SimFin ID and insert into the SQL Database
API Info: https://simfin.com/api/v1/documentation/#operation/getPrices
"""

from urllib.request import Request, urlopen
import urllib
import requests
import SQLConnect


def chunks(the_list, size):
    "yield successive size-sized chunks from the_list."
    for i in range(0, len(the_list), size):
        yield the_list[i:(i+size)]

def generate_insert_str(table, keys):
    column_str = ', '.join([key for key in keys])
    values_str = ('?, ' * len(keys))[:-2]

    insert_str = """MERGE %s USING (
                    VALUES
                        (%s)
                    ) AS vals (%s)
                    ON simfin.daily_price.simId = vals.simId and simfin.daily_price.date = vals.date
                    WHEN MATCHED THEN
                        UPDATE SET
                        closeAdj = vals.closeAdj,
                        splitCoef = vals.splitCoef
                    WHEN NOT MATCHED THEN
                        INSERT (%s)
                        VALUES (vals.simId, vals.date, vals.closeAdj, vals.splitCoef);
                  """ % (table, values_str, column_str, column_str)
    return insert_str

def check_primary_key(table, simId, start_date, end_date):
    """
    Query the DB to see if the any dates in the requested date range
    are already in the database. Return True if data already exists within
    the range, False otherwise.
    """
    query_str = """
                SELECT TOP 1 1
                FROM %s
                WHERE
                    [simId] = %d and [date] BETWEEN '%s' and '%s'
                """ % (table, simId, start_date, end_date)
    if not sqlconnect.query_db(query_str):
        return True
    else:
        return False

def insert_daily_price_data(sqlconnect, simIds, table, columns, start_date, end_date):

    task =  {'start': start_date, "end": end_date}
    insert_str = generate_insert_str(table, columns)

    num_ids = len(simIds)
    counter = 1

    for simId_subset in chunks(simIds, 25):
        data = []
        for i, simId in enumerate(simId_subset):
            print(counter, '/', num_ids)

            url  = 'https://simfin.com/api/v1/companies/id/' + repr(simId) + '/shares/prices?api-key=' + API_KEY
            response = requests.get(url, task)
            result = response.json()

            if result == {'error': 'no share price data found for company'}:
                print(result)
            elif result == {'error': 'no share classes found for company'}:
                print(result)
            else:
                priceData = result['priceData']                     # extract just the prices
                priceData = [(simId, *tuple(price.values()))        # add the simId to the price tuples
                                 for simId, price in zip([simId]*len(result['priceData']), result['priceData'])]
                data.append(priceData)
            counter += 1

    data = [date for company in data for date in company] # unpack list of lists into one large list
    if data:
        sqlconnect.reset_cursor()                   # try to avoid the host forced closing of the connection
        sqlconnect.curs.fast_executemany = True
        print('Merging/Inserting data...')
        sqlconnect.curs.executemany(insert_str, data)
        print('Done!')
        sqlconnect.curs.commit()
    else:
        print('No data found!')

    sqlconnect.curs.close()
    sqlconnect.conn.close()

if __name__ == '__main__':
    """
    Performs below logic when run as a script. First argument is a simId (int),
    second argument is the start date (YYYY-MM-DD), third argument is the end date
    (YYYY-MM-DD).

    Must provide an end date if provided a start date. If no dates provided,
    a default date range is used.
    """

    import sys
    if len(sys.argv) > 1:
        "Use the passed in simId"
        try:
            simIds = [int(sys.argv[1])]
        except ValueError:
            print('First argument (simId) must be an integer!')
            sys.exit()
    else:
        "Use all of the simIds"
        querystr = """
        SELECT simId
        FROM SIMFIN.general_company_data
        """
        simIds = sqlconnect.query_db(querystr)
        simIds = [int(sid[0]) for sid in simIds]
    if len(sys.argv) > 2:
        start_date = sys.argv[2]
        if len(sys.argv) < 4:
            print('Must provide end date with start date!')
            sys.exit()
        else:
            end_date = sys.argv[3]
    else:
        start_date = '2017-01-01'
        end_date = '2017-01-31'
    print('Date Range: %s to %s' % (start_date, end_date))

    API_KEY = 'N8XsZtAZtjMH8aUVK0d4A6HXm152V0TF'
    sqlconnect = SQLConnect.SQLConnect()
    table = 'SimFin.daily_price'
    keys = ['simId', 'date', 'closeAdj', 'splitCoef']

    insert_daily_price_data(sqlconnect, simIds, table, keys, start_date, end_date)
