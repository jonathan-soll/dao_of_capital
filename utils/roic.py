import pandas as pd
from utils import SQLConnect



def return_roic_df(simIds):
    sqlconnect = SQLConnect.SQLConnect()

    balance_sheet_querystr = """
    SELECT *
    FROM SimFin.vw_InvestedCapital t (nolock)
    """

    income_statement_querystr = """
    SELECT *
    FROM SimFin.vw_EBIT t (nolock)
    """

    df_balancesheet = pd.read_sql(balance_sheet_querystr, sqlconnect.conn)
    df_incomestatement = pd.read_sql(income_statement_querystr, sqlconnect.conn)

    df_result = pd.DataFrame()

    for simId in simIds:
        df_ticker_bs = df_balancesheet[df_balancesheet['simId'] == simId].sort_values(by='fiscal_year')
        df_ticker_bs['Avg_Invested_Capital'] = (df_ticker_bs['Invested Capital'] + df_ticker_bs['Invested Capital'].shift()) / 2

        df_ticker_is = df_incomestatement[df_incomestatement['simId'] == simId].sort_values(by='fiscal_year')

        df_ticker_roic = df_ticker_bs.merge(df_ticker_is)
        df_ticker_roic['ROIC'] = df_ticker_roic['EBIT'] / df_ticker_roic['Avg_Invested_Capital']

        if df_result.empty:
            df_result = df_ticker_roic.dropna()
        else:
            df_result = df_result.append(df_ticker_roic.dropna())

    return df_result
