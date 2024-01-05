import pandas
import logging
from src import config, dbconnection as db
from matplotlib import pyplot as plt

logging.basicConfig(
    filename='log.log', 
    filemode='a', 
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Define function to analyse the data from the logs generated
def analyse_data():
    '''
    This function will analyze the data from the logs generated
    and generate a report
    '''
    # Sucess
    sucess = True
    
    # Load configuration
    config_info = config.load_config()
    database = config_info['database']
    tables = config_info['tables']
    columns = config_info['columns']

    table = tables[1] # Logtable is the second table from config file
    
    values = db.select(
        database = database['name'],
        usr = database['username'],
        password = database['password'],
        table_name=table
    )

    # Analyze the data: count occurrences of each command in table
    command_counts = values[values['id']>500]['command'].value_counts()
    
    # Print out the analysis
    print("Counts of Each Type of Interaction:")
    print(command_counts)

    
    command_counts.plot(kind='bar')
    plt.title('Counts of Each Type of Interaction')
    plt.xlabel('Command Type')
    plt.ylabel('Counts')
    plt.xticks(rotation=45)  # Rotate labels for better readability
    plt.show()
    

    return sucess

