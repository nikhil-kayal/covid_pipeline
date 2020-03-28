import io
import json
import requests
import pandas as pd


class County(object):

    def __init__(self, confirmed_case_url, death_case_url, testing_cases_url, rolling_period):
        self.confirmed_case_url = confirmed_case_url
        self.death_case_url = death_case_url
        self.testing_cases_url = testing_cases_url
        self.rolling_period = rolling_period

    @staticmethod
    def read_county_population(df):
        county_population = pd.read_csv("data/county_populations.csv")
        county_population["county_name_"] = county_population["county_name"].str.split(",\s")
        county_population["state"] = ""
        for i, r in enumerate(county_population["county_name_"]):
            county_population["state"].iloc[i] = r[1]
        with open("data/us_abbr.json") as f:
            us_state_abbrev = json.load(f)
        county_population["stateCode"] = county_population["state"].map(us_state_abbrev)
        county_population = county_population.drop(["county_name_", "state"], axis=1)
        return county_population.join(df.set_index("countyFIPS"), on=["countyFIPS"], how="left"). \
            drop(["County Name", "State", "stateFIPS"], axis=1).reset_index(drop=True)

    def get_confirmed_cases(self):
        content = requests.get(self.confirmed_case_url).content
        confirmed_cases = pd.read_csv(io.StringIO(content.decode("utf-8"))).dropna(how='any', axis=1)
        # confirmed_cases['3/20/2020'].loc[(confirmed_cases['3/20/2020'] == '5,151')] = 5151.0
        return self.read_county_population(confirmed_cases)

    def get_death_cases(self):
        content = requests.get(self.death_case_url).content
        death_cases = pd.read_csv(io.StringIO(content.decode("utf-8"))).dropna(how='any', axis=1)
        return self.read_county_population(death_cases)

    def get_rolling_average(self, df):
        return pd.concat([df[["countyFIPS", "county_name", "population_2018", "stateCode"]],
                          df.drop(["countyFIPS", "county_name", "population_2018", "stateCode"],
                                  axis=1).rolling(self.rolling_period, axis=1).mean()], axis=1)

    @staticmethod
    def get_incidence_rate(df):
        return pd.concat([df[["countyFIPS", "county_name", "population_2018", "stateCode"]],
                          (df.drop(["countyFIPS", "county_name", "population_2018", "stateCode"], axis=1).astype('float'). \
                           div(df.population_2018, axis=0)) * 100000], axis=1)

    def get_temporal_covid_testing_data_by_state(self):
        testing_cases = pd.read_csv(self.testing_cases_url)
        testing_cases["daily_pos"] = 0
        testing_cases["daily_neg"] = 0
        testing_cases["daily_total"] = 0
        testing_cases.fillna(0, inplace=True)
        for state in testing_cases.state.unique():
            indices = testing_cases[testing_cases.state == state].index[::-1]
            max_pos = 0
            max_neg = 0
            for i, idx in enumerate(indices):
                # Use to fill in 0s in the cumulative figures
                if testing_cases.at[idx, "positive"] > max_pos:
                    max_pos = testing_cases.at[idx, "positive"]
                else:
                    testing_cases.at[idx, "positive"] = max_pos
                if testing_cases.at[idx, "negative"] > max_neg:
                    max_neg = testing_cases.at[idx, "negative"]
                else:
                    testing_cases.at[idx, "negative"] = max_neg
                # recalculate total
                testing_cases.at[idx, "total"] = testing_cases.at[idx, "positive"] + testing_cases.at[idx, "negative"]
                # Then calculate daily numbers
                if i == 0:
                    testing_cases.at[idx, "daily_pos"] = testing_cases.at[idx, "positive"]
                    testing_cases.at[idx, "daily_neg"] = testing_cases.at[idx, "negative"]
                    testing_cases.at[idx, "daily_total"] = testing_cases.at[idx, "total"]
                else:
                    testing_cases.at[idx, "daily_pos"] = testing_cases.at[idx, "positive"] - \
                                                         testing_cases.at[indices[i-1], "positive"]
                    testing_cases.at[idx, "daily_neg"] = testing_cases.at[idx, "negative"] - \
                                                         testing_cases.at[indices[i-1], "negative"]
                    testing_cases.at[idx, "daily_total"] = testing_cases.at[idx, "total"] - \
                                                           testing_cases.at[indices[i-1], "total"]
        testing_cases["pct_pos"] = testing_cases["positive"] / testing_cases["total"]
        testing_cases["daily_pct_pos"] = testing_cases["daily_pos"] / testing_cases["daily_total"]
        testing_cases["date"] = pd.to_datetime(testing_cases["date"], format="%Y%m%d").dt.strftime("%m/%d/%Y")
        testing_cases = testing_cases.drop(["dateChecked"], axis=1)
        return testing_cases

    def process_all(self):
        confirmed_cases = self.get_confirmed_cases()
        death_cases = self.get_death_cases()
        rolling_average_confirmed_cases, rolling_average_death_cases = self.get_rolling_average(confirmed_cases), self. \
            get_rolling_average(death_cases)
        incidence_rate_confirmed_cases = self.get_incidence_rate(confirmed_cases)
        incidence_rate_death_cases = self.get_incidence_rate(death_cases)
        testing_cases =  self.get_temporal_covid_testing_data_by_state()
        return confirmed_cases, death_cases, rolling_average_confirmed_cases, rolling_average_death_cases, \
               incidence_rate_confirmed_cases, incidence_rate_death_cases, testing_cases