import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
from apache_beam.transforms import Map
from sqlalchemy import create_engine
import country_converter as coco
import logging



def transform_data(dictionary):
    return {
        'date': dictionary.get('date', None),  
        'partner': dictionary.get('partner', None),  
        
        'partner_type': dictionary.get('partner_type', None),  
        'unique_clicks': dictionary.get('unique_clicks', None),
        
        'new_registrations': dictionary.get('new_registrations', None),  
        'first_time_depositing': dictionary.get('first_time_depositing', None),
        
        'cpa_triggered': dictionary.get('cpa_triggered', None),  
        'cpa_earnings_eur': dictionary.get('cpa_earnings_eur', None),
        
        'rev_share_earnings_eur': dictionary.get('rev_share_earnings_eur', None),  
        'amount_deposited_eur': dictionary.get('amount_deposited_eur', None),
        
        'net_revenue_eur': dictionary.get('net_revenue_eur', None),  
        'Full_Countries': dictionary.get('Full_Countries', None),
    }


#Reading csv file into pandas dataframe
data2 = pd.read_csv('partners_data.csv')

print('Size of DataFrame',data2.shape)

#Data Cleaning and Checking

converter = coco.CountryConverter()
full_countries = converter.convert(names=data2['country'], to = 'name_official')
data2['Full_Countries'] = full_countries


string_to_discard_1 = "Casino"
string_to_discard_2 = "Bookmakers"

for ind in data2.index:
    x = data2['partner'][ind].replace(string_to_discard_1,"")
    y = x.replace(string_to_discard_2,"")
    data2['partner'][ind] = y



#Converting dataframe to dictionary 
data2_list = data2.to_dict(orient='records')


# Defining the pipeline options
pipeline_options = PipelineOptions(
    runner='DirectRunner'
    
)

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    pcoll_data = (
        p | "Create PCollection" >> beam.Create(data2_list)
    )

  
    transformed_data = (
        pcoll_data
        | "Transform Data" >> Map(transform_data)
    )






def write_to_postgres(elements, connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        for element in elements:
            try:
                connection.execute("INSERT INTO my_table (date, partner, partner_type, unique_clicks, new_registrations, first_time_depositing, cpa_triggered, cpa_earnings_eur, rev_share_earnings_eur, amount_deposited_eur, net_revenue_eur, Full_Countries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   element['date'], element['partner'], element['partner_type'], element['unique_clicks']
                                   , element['new_registrations'], element['first_time_depositing'], element['cpa_triggered'], 
                                   element['cpa_earnings_eur'], element['rev_share_earnings_eur'],
                                   element['amount_deposited_eur'], element['net_revenue_eur'], element['Full_Countries'])
                logging.info("Inserted data into PostgreSQL successfully.")
            except Exception as e:
                print(logging.error(f"Error inserting data into PostgreSQL: {str(e)}"))

connection_string = 'postgresql://postgres:bluewindow@34.155.139.190:5432/postgres'

with beam.Pipeline(options=pipeline_options) as p:
    
    transformed_data | "Write to PostgreSQL" >> beam.ParDo(write_to_postgres, connection_string)
    
    
    
    
    
    
    
    
    
    

