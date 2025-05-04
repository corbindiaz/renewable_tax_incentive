import pandas as pd
from skimpy import skim 

all_state_incentives = pd.read_csv("all_state_incentives.csv")
data_48c = pd.read_csv("48C_CensusTractDesignation.csv")
nrel_laws_incentives = pd.read_csv("nrel_laws_incentives.csv")

# first changing datasets to have the same state name 
abbr_to_state = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",  "US": "United States of America", "DC": "District of Columbia"
}

def abbreviation_to_state(abbreviation):
    if isinstance(abbreviation, str):
        abbreviation = abbreviation.upper().strip()  # Make sure it's clean
        return abbr_to_state.get(abbreviation, "State not found")
    else:
        return abbreviation  # leave non-strings unchanged


# Directly modify the 'State' column with abbreviations
nrel_laws_incentives['State'] = nrel_laws_incentives['State'].apply(abbreviation_to_state)
nrel_laws_incentives.to_csv("nrel_laws_incentives.csv", index = False)

# getting year and changing name of state values for all of these 
data_48c['date_last_'] = pd.to_datetime(data_48c['date_last_'])
data_48c['year'] = data_48c['date_last_'].dt.year
data_48c = data_48c.rename(columns={'State_Name': 'State'})
data_48c.to_csv("48C_CensusTractDesignation.csv", index = False)

nrel_laws_incentives['year'] = pd.to_datetime(nrel_laws_incentives['Status Date']).dt.year
nrel_laws_incentives['year'] = nrel_laws_incentives['year'].astype('Int64')
nrel_laws_incentives.to_csv("nrel_laws_incentives.csv", index = False)

all_state_incentives['year'] = pd.NA
all_state_incentives = all_state_incentives.rename(columns={'location': 'State'})
all_state_incentives.to_csv("all_state_incentives.csv", index = False)

data_48c.columns = data_48c.columns + '_data48c'
nrel_laws_incentives.columns = nrel_laws_incentives.columns + '_nrel_incentives'
all_state_incentives.columns = all_state_incentives.columns + '_state_incentives'
data_48c = data_48c.rename(columns={'State_data48c': 'State', 'year_data48c': 'year'})
nrel_laws_incentives = nrel_laws_incentives.rename(columns={'State_nrel_incentives': 'State', 'year_nrel_incentives': 'year'})
all_state_incentives = all_state_incentives.rename(columns={'State_state_incentives': 'State', 'year_state_incentives': 'year'})


merged = pd.merge(data_48c, nrel_laws_incentives, on = ['State', 'year'], how = "outer", suffixes = ('_data48c', '_nrel_incentives'))
merged = pd.merge(merged, all_state_incentives, on = ["State", "year"], how = 'outer', suffixes = ('', 'all_state_incentives'))
merged.insert(0, 'year', merged.pop('year'))  # Move 'year' to the front
merged.insert(1, 'state', merged.pop('State'))
merged.columns = merged.columns.str.lower()
merged['year'] = merged['year'].astype('Int64')
merged['law id_nrel_incentives'] = merged['law id_nrel_incentives'].astype('Int64')
merged['sequence number_nrel_incentives'] = merged['sequence number_nrel_incentives'].astype('Int64')

merged = merged.drop(columns = ['oid__data48c', 'ctract_geo_data48c', 'state_fip_data48c', 'county_fip_data48c', 'tract_fip_data48c', 'tract_name_data48c', 
                                'f48c_tract_data48c', 'dataset_ve_data48c', 'shape_leng_data48c', 'shape_area_data48c', 'topic_nrel_incentives', 'technology categories_nrel_incentives', 'user categories_nrel_incentives'])

columns_to_exclude = ['state', 'year']

# Select all columns except the ones to exclude and handle NaN values
merged['Combined'] = merged.drop(columns=columns_to_exclude).apply(
    lambda row: '|'.join(row.dropna().astype(str)), axis=1
)

# Show only the excluded columns and the new 'Combined' column
merged_filtered = merged[columns_to_exclude + ['Combined']]
# Display the filtered DataFrame
#print(merged_filtered)
print(merged_filtered.head())
print(merged.head())

merged.to_csv("chelsea_merged_datasets.csv", index = False)
merged_filtered.to_csv("chelsea_merged_datasets_2.csv", index = False)





