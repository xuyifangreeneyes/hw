import mysql.connector
import time
import random
from PIL import Image
import numpy as np

def img2arr(img_path):
    img = Image.open(img_path)
    return np.array(img.convert('L'))

if __name__ == '__main__':
    arr = img2arr('lisa_small.png')

    mydb = mysql.connector.connect(
        host='127.0.0.1', 
        port=4000, 
        user='root')
    mycursor = mydb.cursor()
    mycursor.execute('CREATE DATABASE IF NOT EXISTS yifan_db')
    mydb.commit()
    mycursor.execute('use yifan_db')
    mydb.commit()

    num_tables = arr.shape[0]
    num_epochs = arr.shape[1]
    num_records = 100
    max_value = 255.0
    name_prefix = 'may force be with you!!'

    print('create tables...')
    for tid in range(num_tables):
        mycursor.execute('CREATE TABLE mytable{} (id INT PRIMARY KEY, name VARCHAR(255))'.format(tid))
        mydb.commit()

    print('write to tables...')
    for tid in range(num_tables):
        print('table {}'.format(tid))
        sql = 'INSERT INTO mytable{} (id, name) VALUES (%s, %s)'.format(tid)
        t1 = time.time()
        for rid in range(num_records):
            val = (str(rid), name_prefix + ' rid = {} time = {}'.format(rid, t1))
            mycursor.execute(sql, val)
            mydb.commit()
        t2 = time.time()
        print('insert {} records to mytable{} costs {} s'.format(num_records, tid, t2 - t1))


    for eid in range(num_epochs):
        t1 = time.time()
        for tid in range(num_tables):
            # num_updates = random.randint(0, num_records - 1)
            num_updates = np.clip(int(arr[num_tables - tid - 1][eid] / max_value * 100.0), 0, 100)
            print('epoch[{}] mytable{} update {} records'.format(eid, tid, num_updates))
            sql = 'UPDATE mytable{} SET name = %s WHERE id = %s'.format(tid)
            for rid in range(num_updates):
                val = (name_prefix + ' rid = {} time = {}'.format(rid, t1), str(rid))
                mycursor.execute(sql, val)
                mydb.commit()
        t2 = time.time()
        print('epoch[{}] takes {} s'.format(eid, t2 - t1))
        sleep_time = 60 - (time.time() - t1)
        if sleep_time > 0:
            time.sleep(sleep_time)
