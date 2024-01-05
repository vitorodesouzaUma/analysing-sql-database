import src.config as config
import src.dbconnection as db
import logging, random
import pandas as pd

logging.basicConfig(
    filename='log.log', 
    filemode='a', 
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def interact_database():
    
    # Sucess
    sucess = True
    
    # Load configuration
    config_info = config.load_config()
    database = config_info['database']
    tables = config_info['tables']
    columns = config_info['columns']

    table = tables[0] # Datatable is the first table from config file

    # Get only the name of the column in config file
    column_list = [c.split(' ')[0] if ' ' in c else c for c in columns[table]] 
    
    values = db.select(
        database = database['name'],
        usr = database['username'],
        password = database['password'],
        table_name=table
    )

    # Ensure the 'height' column is of type float
    values['height'] = values['height'].astype(float)

    # Single value if INSERT
    value = None

    interactions = {
        'INSERT': 'INSERT INTO {} ({}) VALUES ({})',
        'UPDATE': 'UPDATE {} SET {} WHERE {}',
        'DELETE': 'DELETE FROM {} WHERE {}'
    }

    for i in range(1500):

        # Select interaction
        interaction = random.choice(list(interactions.keys()))

        # Select values
        if interaction == 'INSERT':
            value = values.sample(n=1)
            value = (value['name'].values[0],value['height'].values[0])

            # Format the column list and values for SQL
            column_list_str = ', '.join(column_list[1:])
            values_placeholder = ', '.join(['%s' for _ in value]) # Create placeholders for parameterized query

            # Prepare the SQL query
            query = interactions[interaction].format(table, column_list_str, values_placeholder)


        elif interaction == 'UPDATE':
            column = random.choice(column_list[1:])
            if column == 'name':
                random_name = values['name'].sample(n=1).iloc[0]
                set = f"name = '{random_name}'"
            elif column == 'height':
                mean_height = values['height'].mean()
                std_dev_height = values['height'].std()
                random_height = random.gauss(mean_height, std_dev_height)
                set = f"height = {random_height}"

            random_id = values['id'].sample(n=1).iloc[0]  
            condition = f"id = {random_id}"

            query = interactions[interaction].format(table,set,condition)

        elif interaction == 'DELETE':
            condition = 'id = ' + str(random.randint(1, len(values)))
            query = interactions[interaction].format(table, condition)

        # Execute interaction
        try:
            db.execute(
                database = database['name'],
                usr = database['username'],
                password = database['password'],
                query = query,
                param = value
            )
        except Exception as e:
            logging.error(e)
            sucess = False
            break
            
    return sucess