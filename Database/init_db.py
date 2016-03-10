#!/usr/bin/python

import sqlite3 as lite
import csv

# Create Tables
def create_tables(cur):
    # RESTAURANT
    cur.execute("CREATE TABLE IF NOT EXISTS RESTAURANT(\
                            REST_ID INT PRIMARY KEY, REST_NAME TEXT,\
							REST_TYPE INT, REST_ADD TEXT,\
							REST_PHONE TEXT, REST_STATUS INT,\
							REST_COMMENT TEXT, REST_IMAGE BLOB)")

    # RESTAURANT_TYPE
    cur.execute("CREATE TABLE IF NOT EXISTS RESTAURANT_TYPE(\
                            RTYPE_ID INT PRIMARY KEY, RTYPE TEXT)")

    # RESTAURANT_STATUS
    cur.execute("CREATE TABLE IF NOT EXISTS RESTAURANT_STATUS(\
                            RSTAT_ID INT PRIMARY KEY, RSTAT TEXT)")

    # DISH
    cur.execute("CREATE TABLE IF NOT EXISTS DISH(\
                            DISH_ID INT PRIMARY KEY,\
                            DISH_TYPE INT, DISH_NAME TEXT, DISH_IMAGE BLOB)")
   
    # DISH_TYPE
    cur.execute("CREATE TABLE IF NOT EXISTS DISH_TYPE(\
                            DTYPE_ID INT PRIMARY KEY, DTYPE TEXT)")

	# DISH_COMMENT
    cur.execute("CREATE TABLE IF NOT EXISTS DISH_COMMENT(\
                            DC_ID INT PRIMARY KEY,\
                            DISH_ID INT, DC_TASTE REAL, DC_SPICE REAL, DC_VAL_FOR_MONEY REAL,\
                            DC_RATING REAL, DC_COMMENT TEXT, DC_IMAGE BLOB)")

	# MEAL
    cur.execute("CREATE TABLE IF NOT EXISTS MEAL(\
                            M_ID INT PRIMARY KEY,\
                            M_TYPE INT, BILL_NO TEXT, DC_ID TEXT, M_QOS REAL,\
                            M_RATING REAL, M_COMMENT TEXT)")

    # MEAL_TYPE
    cur.execute("CREATE TABLE IF NOT EXISTS MEAL_TYPE(\
                            MTYPE_ID INT PRIMARY KEY, MTYPE TEXT)")


# Load dish data
def load_dish(cur):
    try:
        creader = csv.reader(open('Dish.csv', 'rb'), delimiter=',', quotechar='|')
        for t in creader:
            row = [unicode(t[0], "utf8"), unicode(t[1], "utf8"), 
                    unicode(t[2], "utf8"), unicode(t[3], "utf8")]
            cur.execute('INSERT INTO DISH VALUES (?,?,?,?)', row)
    except IOError, e:
        print 'load_dish - I/O Error: ', e
    except Exception, e:
        print 'load_dish - Error: ', e
    else:
        print 'load_dish: Successfully imported'


# Load restaurant data
def load_restaurants(cur):
    try:
        creader = csv.reader(open('Restaurant.csv', 'rb'), delimiter=',', quotechar='|')
        for t in creader:
            row = [unicode(t[0], "utf8"), unicode(t[1], "utf8"), 
                    unicode(t[2], "utf8"), unicode(t[3], "utf8"),
                    unicode(t[4], "utf8"), unicode(t[5], "utf8"),
                    unicode(t[6], "utf8"), unicode(t[7], "utf8")]
            cur.execute('INSERT INTO RESTAURANT VALUES (?,?,?,?,?,?,?,?)', row)
    except IOError, e:
        print 'load_restaurants - I/O Error: ', e
    except Exception, e:
        print 'load_restaurants - Error: ', e
    else:
        print 'load_restaurants: Successfully imported'


# Load types and status data. These are static tables
def load_types(cur, tablename, filename):
    sql_stmt = 'INSERT INTO %s VALUES (?,?)' % tablename

    try:
        creader = csv.reader(open(filename, 'rb'), delimiter=',', quotechar='|')
        for t in creader:
            row = [unicode(t[0], "utf8"), unicode(t[1], "utf8")]
            cur.execute(sql_stmt, row)
    except IOError, e:
        print 'load_types - I/O Error: ', e
    except Exception, e:
        print 'load_types - Error: ', e
    else:
        print 'load_types: Successfully imported'



# Connect to the database
con = lite.connect('food.db')

with con:
    cur = con.cursor()    
    cur.execute('SELECT SQLITE_VERSION()')
    
    data = cur.fetchone()
    
    print "SQLite version: %s" % data

    create_tables(cur)

    # Import data into tables
    load_restaurants(cur)
    load_dish(cur)
    load_types(cur, 'RESTAURANT_TYPE', 'Restaurant_Type.csv')
    load_types(cur, 'RESTAURANT_STATUS', 'Restaurant_Status.csv')
    load_types(cur, 'DISH_TYPE', 'Dish_Type.csv')
    load_types(cur, 'MEAL_TYPE', 'Meal_Type.csv')