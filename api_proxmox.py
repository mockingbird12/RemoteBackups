from proxmoxer import ProxmoxAPI
import time
import mysql.connector
import config
import datetime


class MySQLCursor:
    table_name = None

    def __init__(self, table_name):
        self.mydb = mysql.connector.connect(user=config.db_user, password=config.db_passwd, host=config.db_host,
                                       database=config.database)
        self.mycursor = self.mydb.cursor()
        self.table_name = table_name
        print(self)

    def create_table(self) -> None:
        self.mycursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (id INT AUTO_INCREMENT PRIMARY KEY, prox_name VARCHAR(255), "
            f"vm_name VARCHAR(255), vm_id INT, backup_date VARCHAR(255), backup_size BIGINT, time_diff INT)")

    def drop_table(self) -> None:
        print(self)
        self.mycursor.execute(f"DROP TABLE {self.table_name}")

    def insert_data(self, data_list: list) -> None:
        for one_row in data_list:
            sql = (f"INSERT INTO {self.table_name} (prox_name, vm_name, vm_id, backup_date, backup_size, time_diff) "
                   f"VALUES (%s,%s,%s,%s,%s,%s)")
            val = (one_row.get('prox_name'), one_row.get('name'), one_raw.get('vmid'), one_row.get('ctime'),
                   one_row.get('size'), one_row.get('time_diff'))
            print(val)
            self.mycursor.execute(sql, val)
            self.mydb.commit()


def get_storage_content(proxmox, NODE_NAME, STORAGE_NAME) -> list:
    print('STORAGE', STORAGE_NAME)
    return proxmox.nodes(NODE_NAME).storage(STORAGE_NAME).content.get()


def get_backup_date(ctime: int) -> str:
    return datetime.datetime.fromtimestamp(ctime).strftime('%Y-%m-%d %H:%M:%S')

def get_time_diff(ctime: int) -> int:
    return int((time.time() - ctime) / 3600)

my_sql_obj = MySQLCursor(config.remote_backup_table)
my_sql_obj.drop_table()
my_sql_obj.create_table()
for one_storage in config.PROX_LIST:
    res = []
    print('storage', one_storage)
    NODE_NAME = one_storage.get('pve')
    STORAGE_NAME = one_storage.get('prox_storage')
    proxmox = ProxmoxAPI(one_storage.get('prox_ip'), user=one_storage.get('prox_user'),
                         password=one_storage.get('prox_passwd'), verify_ssl=False, service='PVE')
    data = get_storage_content(proxmox, NODE_NAME, STORAGE_NAME)
    one_raw = {}
    # print('data', data)
    for i in data:
        time_diff = get_time_diff(i.get('ctime'))
        one_raw = {'prox_name': one_storage['prox_name'], 'vmid': i.get('vmid'), 'ctime': get_backup_date(i.get('ctime')), 'name': i.get('notes'),
                   'volid': i.get('volid').split('backup')[-1], 'size': i.get('size'), 'time_diff': time_diff}
        res.append(one_raw)
    my_sql_obj.insert_data(res)