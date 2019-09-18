# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 20:55:10 2019

@author: Abhis
"""


import pandas as pd
import pymysql.cursors


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')
host = config.get('main', 'host')
user = config.get('main', 'user')
password = config.get('main', 'password')
db = config.get('main', 'db')



con = con = pymysql.connect(host=host,
                     user=user,            
                     passwd=password,  
                     db=db,
                     charset='utf8mb4',
                     cursorclass=pymysql.cursors.DictCursor,
                     autocommit=True)



with con.cursor() as cur:
    cur.execute('''SELECT  product_name, group_concat(product_id), group_concat(product_variant) as var FROM product
                    WHERE product_variant IS NOT NULL GROUP BY product_name HAVING count(*)>1;''')
    var = []
    for row in cur.fetchall():
        var.append(row['var'].split(','))
        
    for item in var:
        for id_1 in item:
            for id_2 in item:
                if id_1 == id_2:
                    continue
                cur.execute('''INSERT INTO `smart_sales_app`.`product_variant_map`
                                (`product_product_id_1`,
                                `product_product_id_2`)
                                VALUES
                                (%s,%s);''',(id_1,id_2))
                print("variants inserted")
            
    