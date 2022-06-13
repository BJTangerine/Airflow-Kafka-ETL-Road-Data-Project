import pandas as pd 
import numpy as np
import requests                 # for API call.
from bs4 import BeautifulSoup   # for web scrape.
from SimpleLogEngine import log # for logging


class Transformer():
    """
    Class for transforming CSV of data consolidated from initial data ingestion bash phases in the Road Traffic Data ETL project.
    CSV data is stored in this class's self.df attribute as a pandas DF.

    Methods
    _______
    add_random_usd_payments():
        Adds randomly generated payment_amount column in USD and converts that column from cents to dollars.
    currency_convert():
        Transformation to different currency if other than USD. Extracts exchange rates using API from exchangeratesapi.io, 
        converts into json, then uses parts of the JSON data to construct a pandas DF with the index being the currency region abbreviation.
    formatting():
        Splits up the timestamp column of self.df into 5 separate date columns. Then re-orders the columns into
        relative groupings.
    get_csv(): exports the pandas DF stored in self.df as a CSV into a specified destination directory.

    """

    def __init__(self, csv):
        """
        Accepts a CSV file name to convert into a pandas DF and stored in Transformer class's self.df attribute.

        Parameters
        __________
        csv : str
            CSV's file path.
        """

        # creates pandas DF and adds column headers, then stores the DF in self.df attribute.
        self.df = pd.read_csv(csv, header=None, index_col=None)
        self.df.rename(
            columns={0:'row_id', 1:'timestamp', 2:'anon_vehicle_num', 3:'vehicle_type', 4:'payment_code_type', 5:'vehicle_code', 6:'axles_num', 7:'tollplaza_id', 8:'tollplaza_code'}, 
            inplace=True)

        # if utilizing API call to change currency:
        self.super_secret_api_key = 'API_key_here'
        self.region_currency = 'USD'

        log(f'Transformer.__init__,{csv},Transformer object,{__file__}')

        print(f'Transformer class object initialized with data stored as pandas DF from CSV ({csv}).')


    def add_random_usd_payments(self):
        """
        Adds randomly generated payment_amount column in USD and converts that column from cents to dollars.
        """

        np.random.seed(42069) # for reproducibility.

        self.df['USD_payment_amount'] = np.random.randint(1, 1000, self.df.shape[0]) # add column of random payment amounts in cents.
        self.df['USD_payment_amount'] = self.df['USD_payment_amount'] / 100 # convert cents to dollars.

        log(f'Transformer.add_random_usd_payments,None,Updated Transformer object DataFrame,{__file__}')

        print('Random USD payment column added.')
        

    def currency_convert(self, currency):
        """ 
        Transformation to different currency if other than USD.
        Extracts exchange rates using API from exchangeratesapi.io, converts into json, then uses parts of the JSON data to construct a
        pandas DF with the index being the currency region abbreviation.

        Parameters
        __________
        region_currency : str
            3-letter all capitalized region currency abbrevation (e.g. "USD", "CNY", "JPY", etc).
        """

        if not isinstance(currency, (str)):
            raise TypeError('Region currency input must be 3-letter string.')
        else:
            self.region_currency = currency.upper()

            if self.region_currency == 'USD':
                raise ValueError('Input must be other than USD since payment column is already generated in USD by default.')
            else:
                # API call:
                url = f"http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key={self.super_secret_api_key}" 
                response = requests.get(url)
                raw_data = response.json() # gets response as JSON.
                
                print('If pandas DataFrame-related error occurs, first check to ensure API key is valid and connection is successful.')

                # converts response into DF of exchange rates.
                exchange_rates_df = pd.DataFrame(columns=['Rate'], data=raw_data['rates'].values(), index=raw_data['rates'].keys())
                
                # finds exchange rate based off specified region_currency and adds as new payment column in self.df, removing the previous USD payment amount column.
                exchange_rate = exchange_rates_df.loc[self.region_currency].item()
                self.df[f'{self.region_currency}_payment_amount'] = round(self.df['USD_payment_amount'] * exchange_rate, 2)
                self.df = self.df.drop(['USD_payment_amount'], axis=1)

                log(f'Transformer.currency_convert,{self.region_currency},Updated Transformer object DataFrame,{__file__}')

                print(f'Currency converted to {self.region_currency}.')


    def formatting(self):
        """
        Splits up the timestamp column of self.df into 5 separate date columns. Then re-orders the columns into
        relative groupings.
        """

        # splitting up timestamp column into multiple date columns.
        self.df[['day_name_of_week', 'month_name', 'day_of_month', 'time', 'year']] = self.df['timestamp'].str.split(expand=True)
        self.df = self.df.drop(['timestamp'], axis=1)

        # re-ordering columns.
        self.df = self.df[['row_id', 'anon_vehicle_num', 'vehicle_type', 'axles_num', 'vehicle_code', 
        'payment_code_type', f'{self.region_currency}_payment_amount', 'tollplaza_id', 'tollplaza_code', 
        'day_name_of_week', 'month_name', 'day_of_month', 'time', 'year']]

        log(f'Transformer.formatting,None,Updated Transformer object DataFrame,{__file__}')

        print(f'DataFrame timestamp column split into multiple date columns and columns re-ordered.')


    def get_csv(self, directory):
        """
        Exports the pandas DF stored in self.df as a CSV to the specified directory destination.

        Parameters
        __________
        directory : str
            Destination directory's path.
        """

        self.df.to_csv(f'{directory}transformed_extracted_data.csv', index=False)

        log(f'Transformer.get_csv,{directory},CSV file,{__file__}')

        print(f'DataFrame exported as "transformed_extracted_data.csv" to "{directory}"')
