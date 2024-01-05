import argparse
from src import initiate, analyse, interact
import logging

logging.basicConfig(
    filename='log.log', 
    filemode='a', 
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--init',  action='store_true',help='initiate the project')
    parser.add_argument('--interact', action='store_true', help='interact with database to generate logs')
    parser.add_argument('-a', '--analyze', action='store_true', help='analyze the data generated')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    
    args = parse_args()

    logging.info(f'Running app with args: {args}')
    

    if args.init:
        logging.info('initiating project')
        print('initiating project')
        if initiate.initiate_project():
            logging.info('project initiated')
            print('project initiated')
        else:
            logging.error('project could not be initiated correctly')
            print('project could not be initiated correctly')

    elif args.interact:
        logging.info('interacting with database')
        print('interacting with database')
        if interact.interact_database():
            logging.info('database interacted')
            print('database interacted')
        else:
            logging.error('database could not be interacted correctly')
            print('database could not be interacted correctly')

    elif args.analyze:
        logging.info('analyzing data')
        print('analyzing data')
        if analyse.analyse_data():
            logging.info('data analyzed')
            print('data analyzed')
        else:
            logging.error('data could not be analyzed correctly')
            print('data could not be analyzed correctly')
    else:
        print('No action specified. Please use -i, --init, -a, --analyze or --interact')