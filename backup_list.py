import os
import mysql.connector
import time
import datetime

db_user='backup_user'
db_passwd='Jnshgdtw64'
db_host='10.0.32.199'
database='backup_database'

mydb = mysql.connector.connect(user=db_user, password=db_passwd,host=db_host,database=database)
mycursor = mydb.cursor()

backup_table="gydra21_backups"
schedule_table="gydra21_schedule"

# mycursor.execute(f"DROP TABLE {backup_table}")
print(os.getuid())
def create_table_backups(table_name):
    mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, backup_name VARCHAR(255), backup_date VARCHAR(255), backup_size FLOAT, vm_name VARCHAR(255), time_diff INT)")

def create_table_schedule(table_name):
    mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, dates VARCHAR(255), vmid VARCHAR(255))")
    pass

def insert_data(backup_name,backup_date,backup_size,vm_name, date):
    sql = f"INSERT INTO {backup_table} (backup_name,backup_date,backup_size,vm_name, time_diff) VALUES (%s,%s,%s,%s,%s)"
    val = (backup_name, backup_date,backup_size, vm_name, date)
    print(val)
    mycursor.execute(sql, val)
    mydb.commit()

def insert_data_schedule(dates, vmid):
        sql = f"INSERT INTO {schedule_table} (dates, vmid) VALUES (%s, %s)"
        val = (dates, vmid)
        mycursor.execute(sql, val)
        mydb.commit()

def get_vm_name(vmid):
        conf_path = f'/etc/pve/nodes/hydra21/qemu-server/{vmid}.conf'
        with open(conf_path) as conf_file:
                for line in conf_file.readlines():
                        if 'name' in line:
                                return line.split()[-1]

def get_backup_name(notes_path):
        if os.path.exists(os.path.join(BACKUP_PATH,notes_path)):
                with open(os.path.join(BACKUP_PATH,notes_path)) as notes:
                        return notes.readline()
        else:
                return ''


def flush_table(table_name):
    sql = f"DELETE FROM {table_name}"
    mycursor.execute(sql)

def parse_backup_job():
    job_path = '/etc/pve/jobs.cfg'
    schedule = ''
    vm_names = []
    with open(job_path) as job_file:
        for line in job_file.readlines():
            if 'schedule' in line:
                schedule = ' '.join(line.split()[1:])
            if 'vmid' in line:
                for vm_id in line.split()[-1].split(','):
                        vm_names.append(get_vm_name(vm_id))
            if schedule and vm_names:
                print(schedule, vm_names)
                insert_data_schedule(schedule,' '.join(vm_names))
                schedule = ''
                vm_naes = []

BACKUP_PATH="/mnt/pve/storage1/dump/"

create_table_backups(backup_table)
create_table_schedule(schedule_table)
flush_table(backup_table)
flush_table(schedule_table)

for backup_name in os.listdir(BACKUP_PATH):
    if backup_name.endswith('zst'):
        time_dif=int((time.time() - os.path.getctime(os.path.join(BACKUP_PATH,backup_name)))/3600)
        vm_name = get_backup_name(backup_name+'.notes')
        backup_date = datetime.datetime.fromtimestamp(os.path.getctime(os.path.join(BACKUP_PATH,backup_name))).strftime('%Y-%m-%d %H:%M:%S')
        backup_size = os.path.getsize(os.path.join(BACKUP_PATH,backup_name))/1024/1024/1024
        insert_data(backup_name=backup_name,backup_date=backup_date,vm_name=vm_name,date=time_dif,backup_size=backup_size)
parse_backup_job()
