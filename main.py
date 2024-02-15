import os
import mysql.connector
import time
import config
import datetime



root_path='/mnt/storage1/ns/'


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


def get_backups(path: str) -> list:
    res = []
    for i in os.listdir(path):
        res.append(i)
    return res


def get_backup_date(prox_backup: str) -> str:
    return datetime.datetime.fromtimestamp(os.path.getctime(prox_backup)).strftime('%Y-%m-%d %H:%M:%S')


def main():
    for prox in list_proxs(root_path):
        print(prox)
        for backup_dir in (get_vm_id_paths(prox)):
            print(get_id_from_path(backup_dir))
            print(get_backups(backup_dir))
            for one_backup in get_backups(backup_dir):
                print(one_backup)
                print(get_backup_date(os.path.join(backup_dir,one_backup)))


main()