"""
Get general company data by Simfin ID and insert into the SQL database.
API Info:  https://simfin.com/api/v1/documentation/#operation/getCompById

WARNING : NOT UPDATED TO HANDLE ALREADY EXISTING DATA
"""

from urllib.request import Request, urlopen
import urllib
import requests
import SQLConnect

API_KEY = 'N8XsZtAZtjMH8aUVK0d4A6HXm152V0TF'

sqlconnect = SQLConnect.SQLConnect()

querystr = """
SELECT simId
FROM SIMFIN.general_company_data
"""

simIds = sqlconnect.query_db(querystr)
simIds = [int(sid[0]) for sid in simIds]

data = []

for i, simId in enumerate(simIds):
    print(i+1, '/', len(simIds))
    url = 'https://simfin.com/api/v1/companies/id/' + repr(simId) + '/?api-key=' + API_KEY
    response = requests.get(url)
    result = response.json()
    data.append(result)


def insert_to_db(schema, table, data):
    """
    'data' must be a list of dicts
    """

    keys = list(data[0].keys())
    tuples = [tuple(company.values()) for company in data]

    column_str = ', '.join([x for x in keys])
    values_str = ('?, ' * len(keys))[:-2]
    insert_str = """INSERT INTO %s.%s (%s)
                    VALUES (%s)""" % (schema, table, column_str, values_str)

    sqlconnect.curs.fast_executemany = True
    sqlconnect.curs.executemany(insert_str, tuples)
    sqlconnect.curs.commit()

insert_to_db('SimFin', 'general_company_data', data)
