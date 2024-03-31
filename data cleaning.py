import pandas as pd
from datetime import datetime
import numpy as np

def multiple_countries(df):
    # Aggregate the countries that have multiple same dates (due to different regions)
    df_filtered = df[df["Country/Region"].isin(repeated_countries)]
    df_last_column = df_filtered.columns.tolist()[2]
    print(df_last_column)
    aggregated_data = df_filtered.groupby(['Country/Region', 'Date']).sum().reset_index()

    # Create a column to calculate daily
    aggregated_data['Daily'] = 0

    for i in range(1, len(aggregated_data)):
        if aggregated_data.iloc[i]['Country/Region'] == aggregated_data.iloc[i - 1]['Country/Region']:
            aggregated_data.at[i, 'Daily'] = aggregated_data.iloc[i][df_last_column] - aggregated_data.iloc[i - 1][
                df_last_column]

    print(aggregated_data.head(20))

    # aggregated_data.to_csv("countries_multiple_recovered.csv", index=False)
    return aggregated_data


if __name__ == "__main__":
    df_confirmed = pd.read_csv("covid19_confirmed_unpivoted.csv")
    df_confirmed["Date"] = pd.to_datetime(df_confirmed['Date'])
    # Did the multiple countries


    df_deaths = pd.read_csv("time_series_covid19_deaths_global.csv")
    df_deaths["Date"] = pd.to_datetime(df_deaths['Date'])
    # Did the multiple countries


    df_recovered = pd.read_csv("time_series_covid19_recovered_global.csv")
    df_recovered["Date"] = pd.to_datetime(df_recovered['Date'])
    # Did the multiple countries


    df_vaccination = pd.read_csv("vaccinations (1).csv")
    df_vaccination["Date"] = pd.to_datetime(df_vaccination['Date'])


    # A list of countries
    df_countries = pd.read_csv("Countries list.csv")
    repeated_countries = df_countries[~(df_countries["Countries with multiple entries"].isna())]["Countries with multiple entries"].tolist()

    # test = multiple_countries(df_recovered)




    # Cleaning
    # Filter those with only single data for [df_confirmed]
    df_confirmed = df_confirmed[~(df_confirmed["Country/Region"].isin(repeated_countries))]
    # Get column name
    df_last_column = df_confirmed.columns.tolist()[2]

    df_confirmed = df_confirmed.sort_values(by = ["Country/Region", "Date"])
    df_confirmed['Daily Data'] = df_confirmed.groupby('Country/Region')[df_last_column].diff().fillna(0)

    print(df_confirmed)
    df_confirmed.to_csv("single_country_confirmed_cases.csv", index = False)
    # df_confirmed.to_csv("testing_single_country_confirmed_cases.csv", index = False)



    # Filter those with only single data [df_deaths]
    df_deaths = df_deaths[~(df_deaths["Country/Region"].isin(repeated_countries))]
    # Get column name
    df_last_column = df_deaths.columns.tolist()[2]

    df_deaths = df_deaths.sort_values(by = ["Country/Region", "Date"])
    df_deaths['Daily Data'] = df_deaths.groupby('Country/Region')[df_last_column].diff().fillna(0)

    print(df_deaths.head(30))
    df_deaths.to_csv("single_country_death.csv", index=False)
    # df_deaths.to_csv("testing_single_country_death.csv", index=False)





    # Filter those with only single data [df_recovered]
    df_recovered = df_recovered[~(df_recovered["Country/Region"].isin(repeated_countries))]
    # Get column name
    df_last_column = df_recovered.columns.tolist()[2]
    # Initialize a new column for daily recovered cases

    df_recovered = df_recovered.sort_values(by = ["Country/Region", "Date"])
    df_recovered["Daily Data"] = df_recovered.groupby('Country/Region')[df_last_column].diff().fillna(0)

    print(df_recovered)
    df_recovered.to_csv("single_country_recovered.csv", index=False)
    df_recovered.to_csv("testing_single_country_recovered.csv", index=False)


    # Vaccination
    df_vaccination
    df_last_column = df_vaccination.columns.tolist()[2]
    df_vaccination = df_vaccination.sort_values(by = ["Country/Region", "Date"])
    df_vaccination["Daily Data"] = df_vaccination.groupby('Country/Region')[df_last_column].diff().fillna(0)

    print(df_recovered)
    df_vaccination.to_csv("vaccination_compiled.csv", index=False)