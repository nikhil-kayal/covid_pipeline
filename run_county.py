import time
import os
import schedule
from county import County
from configparser import ConfigParser, ExtendedInterpolation


def main():
    county = County(county_confirmed_cases_url, county_deaths_url, testing_cases_url, rolling_period)
    confirmed_cases, death_cases, rolling_average_confirmed_cases, rolling_average_death_cases, \
    incidence_rate_confirmed_cases, incidence_rate_death_cases, testing_cases = county.process_all()
    confirmed_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") + "confirmed_cases.csv"), index=False)
    death_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") + "death_cases.csv"), index=False)
    rolling_average_confirmed_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") +
                                                        "rolling_average_confirmed_cases.csv"), index=False)
    rolling_average_death_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") +
                                                    "rolling_average_death_cases.csv"), index=False)
    incidence_rate_confirmed_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") +
                                                       "incidence_rate_confirmed_cases.csv"), index=False)
    incidence_rate_death_cases.to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") +
                                                   "incidence_rate_death_cases.csv"), index=False)
    for i in testing_cases.drop(["date", "state"], axis=1).columns:
        testing_cases.pivot(index="state", columns="date", values="positive").reset_index(drop=False).\
            to_csv(os.path.join(os.getcwd() + config.get("PATHS", "data") + "{}_count_by_state.csv".format(i),
                                ), index=False)


if __name__ == "__main__":
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read("config.cfg")
    rolling_period = config.getint("DEFAULT", "rolling_period")
    county_confirmed_cases_url = config.get("URLS", "confirmed_cases")
    county_deaths_url = config.get("URLS", "death_cases")
    testing_cases_url = config.get("URLS", "testing_cases")
    # main()
    schedule.every().day.at("09:00").do(main)
    while 1:
        schedule.run_pending()
        time.sleep(1)









