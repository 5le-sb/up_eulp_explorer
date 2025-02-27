import streamlit as st
import pandas as pd
import requests
import io
from functools import reduce
import altair as alt

# Defining lists
countydf = pd.read_csv("UP county codes.csv")
counties = countydf.County.tolist()
countieswm = counties + ['Michigan (statewide)']
UPPCOcounties = countydf[countydf.UPPCO == 'x']['County'].tolist()
btypedf = pd.read_csv("building-type-keys.csv")
btypelist = btypedf[btypedf.sector=='com']['user-facing name'].tolist()
upgrades = pd.read_csv("measure_name_crosswalk.csv")
upgradelist = upgrades['2024_comstock_amy2018_release_2_upgrade_name'].tolist()

to_combine=[]

def build_url(county, building_type, upgrade): 
    """
    Constructs the URL for the EULP data lake based on the user's selections.
    """  
    # common variables
    eulp_home = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock'
    eulp_btype = btypedf[btypedf['user-facing name'] == building_type]['program-facing name'].iloc[0] 
    upgrade_id = upgrades[upgrades['2024_comstock_amy2018_release_2_upgrade_name'] == upgrade]['2024_comstock_amy2018_release_2_upgrade_id'].iloc[0]
    eulp_upgrade = str(upgrade_id).zfill(2)
    release = 'comstock_amy2018_release_2'
    year = '2024'
    stock = 'timeseries_aggregates'
    
    # build the URL based on the county
    if county != "Michigan (statewide)":
        tract = countydf[countydf.County == county]['Tract Code'].iloc[0]  
        spec_upgrade = f'/by_county/upgrade={upgrade_id}'
        file_name = f'up{eulp_upgrade}-{tract.lower()}-{eulp_btype}'
        
        url = f'{eulp_home}/{year}/{release}/{stock}{spec_upgrade}/county={tract}/{file_name}.csv'

        return url, tract, eulp_upgrade, eulp_btype
    
    # build the URL for Michigan (statewide)
    elif county == "Michigan (statewide)":
        spec_upgrade=f'/by_state/upgrade={upgrade_id}/state=MI'
        file_name = f'up{eulp_upgrade}-mi-{eulp_btype}'

        url = f'{eulp_home}/{year}/{release}/{stock}{spec_upgrade}/{file_name}.csv'

        return url, 'mi', eulp_upgrade, eulp_btype


def fetch_single_oedi_file(county, building_type, upgrade): 
    """
    Calls build_url and fetches a single file from the OEDI data lake based on the user's selections.
    """
    # build url
    url, tract, eulp_upgrade, eulp_btype = build_url(county, building_type, upgrade)
    
    # send request to manipulate the response
    response = requests.get(url)

    if response.status_code == 200: 
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        # df.to_csv(f'data/raw-{tract}-up{eulp_upgrade}-{eulp_btype}.csv')

        total_represented_str = 'floor_area_represented'

        # set the index
        df = df.set_index(pd.to_datetime(df['timestamp'])).drop('timestamp', axis=1)
        
        # anchor resampling at the beginning of each hour, keeping all timestamps in 2018
        df.index -= pd.DateOffset(minutes=15)
        
        # resample using custom aggregations
        agg_dict = {col: 'sum' for col in df.columns if col.startswith('out')} 
        agg_dict.update({total_represented_str: 'mean'})
        agg_dict.update({'models_used': 'mean'})
        df = df.resample('h').agg(agg_dict) 
        
        # drop savings columns for EUSS 1 and 2
        # TODO: confirm if we want savings columns in the final data
        # if eulp_upgrade != '00': 
        #     savings_cols = df.filter(regex='.*savings.*', axis=1).columns
        #     df.drop(savings_cols, axis=1, inplace=True)
        
        # identify total represented column from the string
        total_represented_col = df[total_represented_str]
        models_used_col = df['models_used']
        
        # drop non kWh columns
        df = df.filter(regex='kwh')

        # drop district columns for com
        # TODO: confirm if we want district heating/cooling in the data
        # df = df.drop(columns=df.filter(like='district').columns)

        # rename the remaining columns to only include fuel type and end use
        # df.columns = df.columns.str.extract(r'out\.(\w+\.\w+)', expand=False)
        # Convert the Index to a Series
        cols_series = df.columns.to_series()
        # Use str.extract to capture the two parts (group 1: first two segments, group 2: optional ".savings")
        extracted = cols_series.str.extract(r'^out\.([\w_]+\.[\w_]+).*?(\.savings)?$')
        # Combine the groups, filling missing suffixes with an empty string
        new_cols = extracted[0] + extracted[1].fillna('')
        # Assign back to df.columns
        df.columns = new_cols
                
        # insert the total represented and models used columns (sqft)
        df.insert(0, total_represented_str, total_represented_col)
        df.insert(0, 'models_used', models_used_col)
            
        # save csv
        # df.to_csv(f'data/{tract}-up{eulp_upgrade}-{eulp_btype}.csv')

        # add dataframe to list
        to_combine.append(df)

    else: 
        st.write(f'Failed to retrieve data for {county}-{building_type}-{upgrade}.'
                 f'\n Status code: {response.status_code}.' 
                 f'\n This county-building-upgrade combination may not exist in the data lake.')


def combine_files(to_combine): 
    """
    Sums the dataframes in the to_combine list.
    """
    finaldf = reduce(lambda x,y: x.add(y,fill_value=0),to_combine)

    return finaldf

def visualize_df(df, season, daytype, fueltype):
    """
    Calls combine_files to combine the dataframes in to_combine and visualizes the data based on the user's selections.
    """
    vizdf = df.reset_index()
    savings_cols = vizdf.filter(regex='.*savings.*', axis=1).columns
    vizdf.drop(savings_cols, axis=1, inplace=True)

    if season == 'summer':
        vizdf = vizdf[vizdf.timestamp.dt.month.isin([6,7,8])]
    elif season == 'winter':
        vizdf = vizdf[vizdf.timestamp.dt.month.isin([12,1,2])]

    if daytype == 'weekday':
        vizdf = vizdf[vizdf.timestamp.dt.weekday.isin([0,1,2,3,4])]
    elif daytype == 'weekend':
        vizdf = vizdf[vizdf.timestamp.dt.weekday.isin([5,6])]

    if fueltype == 'electricity':
        vizdf = vizdf.loc[:, ~vizdf.columns.str.contains('natural_gas|other_fuel|district', case=False)]
    elif fueltype == 'natural gas':
        vizdf = vizdf.loc[:, ~vizdf.columns.str.contains('electricity|other_fuel|district', case=False)]
    elif fueltype == 'other fuel':
        vizdf = vizdf.loc[:, ~vizdf.columns.str.contains('natural_gas|electricity|district', case=False)]
    elif fueltype == 'district cooling':
        vizdf = vizdf.loc[:, ~vizdf.columns.str.contains('natural_gas|electricity|other_fuel|district_heating', case=False)]
    elif fueltype == 'district heating':
        vizdf = vizdf.loc[:, ~vizdf.columns.str.contains('natural_gas|electricity|other_fuel|district_cooling', case=False)]
    elif fueltype == 'all':
        pass

    vizdfm = pd.melt(vizdf,id_vars='timestamp',value_vars=[col for col in vizdf.columns if col not in ['timestamp','models_used','floor_area_represented','district_cooling.total','district_heating.total','electricity.total','natural_gas.total','other_fuel.total','site_energy.total']])
    vizdfm['hour'] = vizdfm['timestamp'].dt.hour
    # TODO: confirm if this is useful or if other timeframes are needed
    averageday = vizdfm.groupby(['hour','variable']).mean().reset_index()
    averageday.rename({'variable':'end-use'},axis=1,inplace=True)
    averageday.value = round(averageday.value,0)
    st.subheader(f'{season}-{daytype}-{fueltype}')

    chart = alt.Chart(averageday).mark_area().encode(
        x="hour",
        y=alt.Y("value", title="kWh"),
        color=alt.Color("end-use").scale(scheme="tableau20"),
        ).interactive()
    
    st.altair_chart(chart)

############################################################################################################
# Page Content
############################################################################################################

st.title("EULP Data Explorer")
st.text("For Commercial Buildings in Michigan and the Upper Peninsula. Data source: NREL End-Use Load Profiles for the U.S. Building Stock - 2024 comstock amy2018 release 2")

with st.sidebar:
    st.header("Quick Selections")
    geoselection = st.selectbox("Select a geographic area", ['Custom','Upper Peninsula','UPPCO 10 counties','Michigan (statewide)'])
    allbuilding = st.checkbox("Select all building types")


with st.form("my_form"):
    container = st.container()
    
    # county selection
    if geoselection == 'Upper Peninsula':
        selected_counties = container.multiselect('Select one or more counties',
        counties, counties)
    elif geoselection == 'UPPCO 10 counties':
        selected_counties = container.multiselect('Select one or more counties',
        counties, UPPCOcounties)
    elif geoselection == 'Michigan (statewide)':
        selected_counties = container.multiselect('Select one or more counties',
        'Michigan (statewide)', ['Michigan (statewide)'])
    else:
        selected_counties =  container.multiselect('Select one or more counties',
            counties)
        
    # building selection
    if allbuilding:
        selected_buildings = container.multiselect("Select one or more building types:",
            btypelist, btypelist)
    else:
        selected_buildings =  container.multiselect('Select one or more building types:',
            btypelist)
        
    # upgrade selection
    selected_upgrade = st.selectbox(
    'Select an upgrade package:',
    upgradelist)

    # Submit button
    submitted = st.form_submit_button("Submit")

    if submitted:
        for county in selected_counties:
            for building in selected_buildings:
                fetch_single_oedi_file(county, building, selected_upgrade)

        finaldf = combine_files(to_combine)
        st.write("Total Models: " + str(int(finaldf.models_used.iloc[0])))
        if finaldf not in st.session_state:
            st.session_state.finaldf = finaldf

# Outside the form
st.header("Download Data")
if submitted:
    @st.fragment
    def fragment_function():
        st.download_button(
        label="Download data as CSV",
        data=st.session_state.finaldf.to_csv(),
        file_name='eulpdownload.csv',
        mime="text/csv",
    )

    fragment_function()

st.header("Daily Average Load Profile")
if submitted:
    @st.fragment
    def vizfrag():
        seasonchoice = st.selectbox(label="Season", options = ['summer','winter'])
        daytypechoice = st.selectbox(label="Day Type", options = ['weekday','weekend'])
        fueltypechoice = st.selectbox(label="Fuel Type", options = ['electricity','natural gas','other fuel','district cooling','district heating','all'])
        visualize_df(st.session_state.finaldf, seasonchoice, daytypechoice, fueltypechoice)
    vizfrag()
