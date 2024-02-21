from proxmoxer import ProxmoxAPI
import time
import mysql.connector
import config

NODE_NAME = 'hydra21'
STORAGE_NAME = 'pbs-storage1'


proxmox = ProxmoxAPI('10.0.32.66', user='zabbix@pve', password='Xim8128175162!', verify_ssl=False, service='PVE')


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
            f"vm_name VARCHAR(255), backup_date VARCHAR(255), backup_size BIGINT)")

    def drop_table(self) -> None:
        print(self)
        self.mycursor.execute(f"DROP TABLE {self.table_name}")

    def insert_data(self, data_list: list) -> None:
        for one_row in data_list:
            sql = f"INSERT INTO {self.table_name} (prox_name,vm_name, backup_date, backup_size) VALUES (%s,%s,%s,%s)"
            val = (one_row.get('prox_name'),one_row.get('name'), one_row.get('ctime'), one_row.get('size'))
            self.mycursor.execute(sql, val)
            self.mydb.commit()

def get_storage_content(NODE_NAME, STORAGE_NAME) -> list:
    return proxmox.nodes(NODE_NAME).storage(STORAGE_NAME).content.get()

my_sql_obj = MySQLCursor(config.remote_backup_table)
my_sql_obj.drop_table()
my_sql_obj.create_table()
res = []
for one_storage in config.PROX_LIST:
    NODE_NAME = one_storage.get('prox_name')
    STORAGE_NAME = one_storage.get('storage_name')
    data = get_storage_content(NODE_NAME, STORAGE_NAME)
    one_raw = {}
    for i in data:
        one_raw = {'prox_name':NODE_NAME , 'vmid':i.get('vmid'), 'ctime':time.ctime(i.get('ctime')), 'name':i.get('notes'),
                   'volid':i.get('volid').split('backup')[-1],'size':i.get('size')}
        res.append(one_raw)
    my_sql_obj.insert_data(res)