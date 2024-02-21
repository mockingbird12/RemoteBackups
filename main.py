import os
import mysql.connector
import time
import config
import datetime


root_path='/mnt/storage1/ns/'

mydb = mysql.connector.connect(user=config.db_user, password=config.db_passwd,host=config.db_host,database=config.database)
mycursor = mydb.cursor()

def create_table(table_name:str) -> None:
    mycursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, prox_name VARCHAR(255), "
        f"vm_id VARCHAR(255), backup_date VARCHAR(255), time_diff INT)")


def insert_data(data_list: list, table_name: str) -> None:
    for one_row in data_list:
        sql = f"INSERT INTO {table_name} (prox_name,vm_id, backup_date, time_diff) VALUES (%s,%s,%s,%s)"
        val = (one_row.get('prox_name'),one_row.get('vm_id'), one_row.get('backup_date'), one_row.get('time_diff'))
        mycursor.execute(sql, val)
        mydb.commit()


def get_time_diff(backup_path: str) -> int:
    return int((time.time() - os.path.getctime(backup_path)) / 3600)

def flush_table(table_name):
    sql = f"DELETE FROM {table_name}"
    mycursor.execute(sql)



def list_proxs(root_path: str) -> list:
    print('list proxs')
    return os.listdir(root_path)


def get_vm_id_paths(prox_path: str) -> list:
    res = []
    for dir_content in os.listdir(os.path.join(root_path,prox_path, 'vm')):
        res.append(os.path.join(root_path,prox_path, 'vm',dir_content))
    return res


def get_id_from_path(path: str) -> str:
    return path.split('/')[-1]


def get_vm_name(vm_id: int) -> str:
    return ''


def get_backups(path: str) -> list:
    """
    Получить список бекапов из папки.
    Удалить папку owner
    :param path:
    :return:
    """
    res = []
    for i in os.listdir(path):
        if 'owner' not in i:
            res.append(i)
    return res


def get_backup_date(prox_backup: str) -> str:
    return datetime.datetime.fromtimestamp(os.path.getctime(prox_backup)).strftime('%Y-%m-%d %H:%M:%S')


def main():
    create_table(config.remote_backup_table)
    flush_table(config.remote_backup_table)
    data_backup = []
    for prox in list_proxs(root_path):
        # print(prox)
        prox_name = prox
        for backup_dir in (get_vm_id_paths(prox)):
            # print(get_id_from_path(backup_dir))
            vm_id = get_id_from_path(backup_dir)
            for one_backup in get_backups(backup_dir):
                backup_name = one_backup
                backup_date = (get_backup_date(os.path.join(backup_dir,one_backup)))
                time_diff = get_time_diff(os.path.join(backup_dir,one_backup))
                insert_dict = {'prox_name': prox,'vm_id': vm_id,'backup_name': backup_name,'backup_date': backup_date,
                               'time_diff': time_diff}
                data_backup.append(insert_dict)
                # print(prox_name, vm_id, backup_name, backup_date)
    insert_data(data_list=data_backup, table_name=config.remote_backup_table)


main()
