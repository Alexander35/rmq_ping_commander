from pinger_stuff import Ping 
from amqp_handler import AMQPHandler
from postgres_handler import PostgresHandler
import asyncio
import json
import datetime
import logging
import json
import ipaddress
import threading

logger = logging.getLogger('rmq_ping_commander')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
bf = logging.Formatter('{asctime} {name} {levelname:8s} {message}', style='{')
handler.setFormatter(bf)
logger.addHandler(handler)

# with open('config.json') as jcf:
#     config = json.load(jcf)

config = {}

config['rmq_host'] = os.environ.get('RMQ_HOST', '')
config['rmq_exchange'] = os.environ.get('RMQ_PING_COMMANDER_RMQ_EXCHANGE', '')
config['rmq_queue_in'] = os.environ.get('RMQ_PING_OPERATOR_RMQ_QUEUE_IN', '')

config['postgres_db_name'] = os.environ.get('DEVICE_CONFIG_BACKUP_DB_NAME', 'device_config_backup_db_name')
config['postgres_username'] = os.environ.get('DEVICE_CONFIG_BACKUP_DB_USER_NAME', 'postgres_username')
config['postgres_password'] = os.environ.get('DEVICE_CONFIG_BACKUP_DB_PASSWORD', 'postgres_password')
config['postgres_host_name'] = os.environ.get('DEVICE_CONFIG_BACKUP_HOST_ADDRESS', '')

PH = PostgresHandler(
    config['postgres_db_name'],
    config['postgres_username'], 
    config['postgres_password'],
    config['postgres_host_name'],
)

def ping_network(network):
    P = Ping(network, 0.04)
    dsting = json.dumps(P.start())
    lstring = json.loads(dsting)
    logger.info(lstring)

    add_potencial_devices(lstring['Available'])

def add_potencial_devices(dev_list):
    logger.info(dev_list)

    for device in dev_list:
        insertion_status = PH.insert(
            'device_config_operator_potentialdevice', 
            (
                'device_ipv4', 'created_at', 'updated_at',
            ), 
            (
                device['Addr'], device['DateTime'], device['DateTime']
            )
        )
        logger.info(' insertion Addr {}  {}  '.format( device['Addr'], device['DateTime'] ))
        logger.info(insertion_status)       

def filter_ip_by_exists(ip_addr):
    # for ip in ip_addresses:
    get_exists_device = []
    sql_get_all_exists_devices = \
        "SELECT id,device_name,device_ipv4 FROM device_config_operator_device where device_ipv4 = '{}' LIMIT 1".format(ip_addr)

    get_exists_device.append(PH.execute(sql_get_all_exists_devices))

    sql_get_all_exists_devices = \
        "SELECT device_ipv4 FROM device_config_operator_potentialdevice where device_ipv4 = '{}' LIMIT 1".format(ip_addr)

    get_exists_device.append(PH.execute(sql_get_all_exists_devices))

    if get_exists_device == []:
        return None
    
    return get_exists_device

def rmq_msg_proc(msg):
    try:

        msg = msg.decode('utf-8')
        msg = json.loads(msg)

        ip_address_list = []

        for addr in ipaddress.IPv4Network(msg['network']).hosts():
            if filter_ip_by_exists(addr.__str__()) == None:
                ip_address_list.append(addr.__str__())

        threading.Thread(target=ping_network, args=(ip_address_list,)).start()

        logger.info(ip_address_list)


        return (True, None)

    except Exception as exc:
        logger.error('ping error : {} '.format(exc))

def main():
    loop = asyncio.get_event_loop()

    AMQPH = AMQPHandler(loop)

    loop.run_until_complete(AMQPH.connect(amqp_connect_string=config['rmq_host']))

    loop.run_until_complete(
        AMQPH.receive(
            config['rmq_exchange'], 
            config['rmq_queue_in'], 
            rmq_msg_proc
        )
    )
    loop.close()

if __name__ == '__main__':
    main()