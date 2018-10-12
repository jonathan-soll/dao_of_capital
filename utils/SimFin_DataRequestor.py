import json
from urllib.request import Request, urlopen
import urllib
import requests
import pandas as pd
import itertools

class SimFin_DataRequestor:
    """
    Makes a connection to the SimFin API. Gives the ability to make API requests for
    financial statement data and to create a dataframe of multiple financial statements
    for multiple companies and time frames.
    """
    def __init__(self, API_KEY):
        """
        Initialize a class to get data from SimFin
        """
        self.API_KEY = API_KEY
        try:
            self.all_companies_df = self.get_all_simfin_companies()
        except Exception as e:
            print('Unable to make request for all SimFin companies! Potentially out of API calls')

    def get_all_simfin_companies(self):
        """
        Return a dataframe that has all of the companies in the SimFin database
        columns = [name, simId, ticker]
        """

        all_companies_url = 'https://simfin.com/api/v1/info/all-entities?api-key=' + self.API_KEY
        all_companies_result = requests.get(all_companies_url).json()
        all_companies_df = pd.DataFrame.from_dict(all_companies_result)
        return all_companies_df

    def _create_combinations(self, tickers, fiscal_years, period_types):
        """
        Return a list of tuples of structure (simId, fiscal_year, period_type) that has
        all combinations for the tickers, fiscal years and period types
        """
        simIds = list(self.all_companies_df[self.all_companies_df['ticker'].isin(tickers)]['simId'])
        combinations = list(itertools.product(simIds, fiscal_years, period_types))
        return combinations

    def get_simfin_data(self, simId, fiscal_year, period_type, statement_type):
        """
        Retrievee financial statement data via the SimFin API

        Parameters:
        simId - the SimFin company id
        fiscal_year - the fiscal year of the financial statement in integer form
        period_type - the SimFin period type:  "Q1", "Q2", "H1", "FY", "TTM", etc.
        statement_type - the SimFin financial statement identifier, "pl", "bs", or "cf"
        """
        url  = 'https://simfin.com/api/v1/companies/id/' + repr(simId) + '/statements/standardised?api-key=' + self.API_KEY
        task =  {'stype': statement_type, "ptype": period_type, "fyear": repr(fiscal_year)}

        response = requests.get(url, task)
        result = response.json()
        return result

    def generate_dataframe(self, tickers, fiscal_years, period_types, statement_type):
        """
        Pull all of the data from SimFin for all of the given tickers, fiscal years, and period types
        and puts it into a dataframe where each row is a ticker/fiscal_year/period_type and the columns
        are the values on the balance sheet at that time

        Parameters:
        tickers - a list of ticker strings
        fiscal_year - the fiscal year of the financial statement in integer form
        period_type - the SimFin period types:  "Q1", "Q2", "H1", "FY", "TTM", etc.
        statement_type - the SimFin financial statement identifier, "pl", "bs", or "cf"
        """

        combinations = self._create_combinations(tickers, fiscal_years, period_types)
        df_isEmpty = 1

        for i, (simId, fiscal_year, period_type) in enumerate(combinations):
            print(i+1, 'out of', len(combinations), 'combinations')
            url = 'https://simfin.com/api/v1/companies/id/' + repr(simId) + '/statements/standardised?api-key=' + self.API_KEY
            try:
                result = self.get_simfin_data(simId, fiscal_year, period_type, statement_type)
            except:
                print(simId, fiscal_year, period_type, 'Something went wrong')
                break

            if result == {'error': 'specified statement was not found for company'}:
                print(simId, fiscal_year, period_type, 'Statement not found for company')
                pass
            else:
                if df_isEmpty == 1:
                    df = pd.DataFrame.from_dict(result['values'])#[['standardisedName', 'valueChosen']]
                    df_isEmpty = 0
                    df['period_type'] = period_type
                    df['fiscal_year'] = fiscal_year
                    df['simId'] = simId
                    df['ticker'] = self.all_companies_df[self.all_companies_df['simId'] == simId]['ticker'].values[0]
                    df['id'] = df[['simId', 'fiscal_year', 'period_type', 'tid']].applymap(str).apply(lambda x: '_'.join(x), axis=1)
                else:
                    df_temp = pd.DataFrame.from_dict(result['values'])#[['standardisedName', 'valueChosen']]
                    df_temp['period_type'] = period_type
                    df_temp['fiscal_year'] = fiscal_year
                    df_temp['simId'] = simId
                    df_temp['ticker'] = self.all_companies_df[self.all_companies_df['simId'] == simId]['ticker'].values[0]
                    df_temp['id'] = df_temp[['simId', 'fiscal_year', 'period_type', 'tid']].applymap(str).apply(lambda x: '_'.join(x), axis=1)

                    df = df.append(df_temp)

        return df
