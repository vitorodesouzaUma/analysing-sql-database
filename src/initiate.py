import src.dbconnection as db
import src.config as config
import csv
import logging

logging.basicConfig(
    filename='log.log', 
    filemode='a', 
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ---------------------------------------------------------------------------------------------------
# Define function to initiate the project
def initiate_project() -> bool:
    '''
    This function will create a database, tables, function and triggers
    in postgres and populate it with the data from the csv file
    Returns: Boolean if sucess
    '''
    # Sucess
    sucess = True
    
    # Load configuration
    config_info = config.load_config()
    database = config_info['database']
    tables = config_info['tables']
    columns = config_info['columns']

    try:
        # Create database
        logging.info('Creating database: ' + database['name'])
        create_db(database)

        # Create tables
        logging.info('Creating tables: ' + str(tables))
        create_tables(database,tables,columns)

        # Create function to attach to the triggers
        function_name = 'f_log_dml_commands'
        logging.info('Creating function: ' + function_name)
        create_function(database,tables,function_name)

        # Create trigger for monitoring DML commands
        trigger_name = 'tr_dml_command_track'
        logging.info('Creating trigger: ' + trigger_name)
        create_triggers(database,tables,function_name,trigger_name)

        # Populate table with some data
        logging.info('Populating tables with data')
        populate_tables(database,tables)

    except Exception as error:
        logging.error('Error: ' + str(error))
        print(error)
        sucess = False

    return sucess

# ---------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------
def create_db(database:dict) -> None:
    '''
    This function will create a database in postgres
    database: Dictionary with the database's name, username and password
    Returns: None.
    '''
    db.create_database(
        database_name=database['name'],
        usr=database['username'],
        password=database['password'],
    )
# ---------------------------------------------------------------------------------------------------
    

# ---------------------------------------------------------------------------------------------------
def create_tables(database:dict,tables:list,columns:dict) -> None:
    '''
    This function will create tables in postgres
    tables: List of Strings
    columns: Dictionary with the columns for each table
    database: Dictionary with the database's name, username and password
    Returns: None.
    '''
    for table in tables:
        db.create_table(
            database = database['name'],
            usr = database['username'], 
            password = database['password'], 
            table_name = table, 
            columns = columns[table],
            table_constraints='PRIMARY KEY(id)'
        )
# ---------------------------------------------------------------------------------------------------
        

# ---------------------------------------------------------------------------------------------------
def create_function(database:dict,tables:list[str],function_name:str) -> None:
    '''
    This function will create functions in postgres
    database: Dictionary with the database's name, username and password
    Returns: None.
    '''

    log_table = tables[1] # the second table is log_table

    query = f'''
    CREATE or REPLACE FUNCTION {function_name}() RETURNS Trigger
    AS
    $$ 
    BEGIN
        
        INSERT INTO "{log_table}" 
            (command,interaction_timestamp,interaction_user) 
        VALUES 
            (TG_OP,NOW(),CURRENT_USER);

    RETURN NEW;
    END
    $$ LANGUAGE plpgsql;
    '''

    db.execute(
        database = database['name'],
        usr = database['username'],
        password = database['password'],
        query = query
    )

# ---------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------
def create_triggers(database:dict,tables:list,function_name:str,trigger_name:str) -> None:
    '''
    This function will create triggers in postgres
    database: Dictionary with the database's name, username and password
    tables: List of Strings
    function_name: String
    trigger_name: String
    Returns: None.
    '''

    datatable = tables[0] # the first table is datatable

    query = f'''
    CREATE or REPLACE TRIGGER {trigger_name} AFTER INSERT or UPDATE or DELETE ON {datatable}
    FOR EACH ROW 
    EXECUTE PROCEDURE {function_name}();
    '''

    db.execute(
        database = database['name'],
        usr = database['username'],
        password = database['password'],
        query = query
    )
# ---------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------
def populate_tables(database:dict,tables:list[str],path:str='data.csv') -> None:
    '''
    This function will populate tables in postgres with csv file
    database: Dictionary with the database's name, username and password
    tables: List of Strings
    path: String
    Returns: None.
    '''
    datatable = tables[0] # the first table is datatable

    with open(path) as csv_file:

        reader = csv.reader(csv_file)
        column_list = next(reader)
        values_list = [tuple(value) for value in reader]

    db.insert_faster(
        database = database['name'],
        usr = database['username'],
        password = database['password'],
        table_name = datatable,
        columns_list= column_list,
        values_list= values_list
    )