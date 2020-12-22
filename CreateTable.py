import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode, cursor
import os
import subprocess


#CREATING TABLES IN DEVELOPMENT (DEV) SCHEMA
try:
   conn = mysql.connector.connect(user='akanksha', password='password', host='127.0.0.1', port = '3306')
   conn.autocommit = False
   cursor_ = conn.cursor()
   
   print('Creating 13 databases of y2008-y2020 and dir database....')
   sql_query = """ CREATE DATABASE y2008 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2009 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2010 """
   cursor_.execute(sql_query)
   conn.commit()


   sql_query = """ CREATE DATABASE y2011 """
   cursor_.execute(sql_query)
   conn.commit()


   sql_query = """ CREATE DATABASE y2012 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2013 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2014 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2015 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2016 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2017 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2018 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2019 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE y2020 """
   cursor_.execute(sql_query)
   conn.commit()

   sql_query = """ CREATE DATABASE dir """
   cursor_.execute(sql_query)
   conn.commit()

   print()
   print('Creating the 10 Ticker List tables TL_1 upto TL_10')

   sql_query = """CREATE TABLE dir.`TL_1` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_2` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_3` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_4` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_5` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_6` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_7` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_8` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_9` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`TL_10` (
  `Stock_ID` INT AUTO_INCREMENT PRIMARY KEY,
  `Symbol` CHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=1; """
   cursor_.execute(sql_query)

   print()
   print('Creating the DIR tables year, month, day, timestamp')

   sql_query = """CREATE TABLE dir.`year_` (
  `YID` CHAR(3) NOT NULL,
  `YEAR_col` year NOT NULL,
  PRIMARY KEY (`YID`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`month_` (
  `mid` CHAR(3) NOT NULL,
  `month_col` int NOT NULL,
  PRIMARY KEY (`mid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`day_` (
  `did` CHAR(3) NOT NULL,
  `day_col` int NOT NULL,
  PRIMARY KEY (`did`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci """
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE dir.`timestamp` (
  `tid` CHAR(5) NOT NULL,
  `time_col` CHAR(15) NOT NULL,
  PRIMARY KEY (`tid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci """
   cursor_.execute(sql_query)

   print()
   print('Creating the y2008-y2020 130 tables from DTP_1 to DTP_10')

   sql_query = """CREATE TABLE y2008.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2008.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2009.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2010.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2011.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2012.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2013.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2014.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2015.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2016.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2017.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2018.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2019.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_1` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_2` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_3` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_4` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_5` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_6` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_7` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_8` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_9` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   sql_query = """CREATE TABLE y2020.`DTP_10` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `ds_id` CHAR(12),
  `open` FLOAT DEFAULT NULL,
  `high` FLOAT DEFAULT NULL,
  `low`  FLOAT DEFAULT NULL,
  `close` FLOAT DEFAULT NULL,
  `volume` DECIMAL(15,3) DEFAULT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; """ 
   cursor_.execute(sql_query)

   print()
   print('Populating the year, month, day and timestamp tables..')
   cursor_.executemany(""" INSERT INTO DIR.YEAR_ (yid, year_col) VALUES (%s,%s)""",[
       ('01',2008),
       ('02',2009),
       ('03',2010),
       ('04',2011),
       ('05',2012),
       ('06',2013),
       ('07',2014),
       ('08',2015),
       ('09',2016),
       ('10',2017),
       ('11',2018),
       ('12',2019),
       ('13',2020)])

   cursor_.executemany(""" insert into dir.month_ (mid, month_col) values (%s,%s)""",[
       ('01',1),
       ('02',2),
       ('03',3),
       ('04',4),
       ('05',5),
       ('06',6),
       ('07',7),
       ('08',8),
       ('09',9),
       ('10',10),
       ('11',11),
       ('12',12)])

   cursor_.executemany("""insert into dir.day_ (did,day_col) values (%s,%s)""",[
       ('01',1),
       ('02',2),
       ('03',3),
       ('04',4),
       ('05',5),
       ('06',6),
       ('07',7),
       ('08',8),
       ('09',9),
       ('10',10),
       ('11',11),
       ('12',12),
       ('13',13),
       ('14',14),
       ('15',15),
       ('16',16),
       ('17',17),
       ('18',18),
       ('19',19),
       ('20',20),
       ('21',21),
       ('22',22),
       ('23',23),
       ('24',24),
       ('25',25),
       ('26',26),
       ('27',27),
       ('28',28),
       ('29',29),
       ('30',30),
       ('31',31)])
   
   cursor_.executemany(""" INSERT INTO dir.timestamp (tid,time_col) values (%s,%s) """, [
       ('001' , '09:30:00'),
       ('002',	'09:31:00'),
       ('003',	'09:32:00'),
       ('004',	'09:33:00'),
       ('005',	'09:34:00'),
       ('006',	'09:35:00'),
       ('007',	'09:36:00'),
       ('008',	'09:37:00'),
       ('009',	'09:38:00'),
       ('010','09:39:00'),
       ('011','09:40:00'),
       ('012','09:41:00'),
       ('013','09:42:00'),
       ('014','09:43:00'),
       ('015','09:44:00'),
       ('016','09:45:00'),
       ('017','09:46:00'),
       ('018','09:47:00'),
       ('019','09:48:00'),
       ('020','09:49:00'),
       ('021','09:50:00'),
       ('022','09:51:00'),
       ('023','09:52:00'),
       ('024','09:53:00'),
       ('025','09:54:00'),
       ('026','09:55:00'),
       ('027','09:56:00'),
       ('028','09:57:00'),
       ('029','09:58:00'),
       ('030','09:59:00'),
       ('031','10:00:00'),
       ('032','10:01:00'),
       ('033','10:02:00'),
       ('034','10:03:00'),
       ('035','10:04:00'),
       ('036','10:05:00'),
       ('037','10:06:00'),
       ('038','10:07:00'),
       ('039','10:08:00'),
       ('040','10:09:00'),
       ('041','10:10:00'),
       ('042','10:11:00'),
       ('043','10:12:00'),
       ('044','10:13:00'),
       ('045','10:14:00'),
       ('046','10:15:00'),
       ('047','10:16:00'),
       ('048','10:17:00'),
       ('049','10:18:00'),
       ('050','10:19:00'),
       ('051','10:20:00'),
       ('052','10:21:00'),
       ('053','10:22:00'),
       ('054','10:23:00'),
       ('055','10:24:00'),
       ('056','10:25:00'),
       ('057','10:26:00'),
       ('058','10:27:00'),
       ('059','10:28:00'),
       ('060','10:29:00'),
       ('061','10:30:00'),
       ('062','10:31:00'),
       ('063','10:32:00'),
       ('064','10:33:00'),
       ('065','10:34:00'),
       ('066','10:35:00'),
       ('067','10:36:00'),
       ('068','10:37:00'),
       ('069','10:38:00'),
       ('070','10:39:00'),
       ('071','10:40:00'),
       ('072','10:41:00'),
       ('073','10:42:00'),
       ('074','10:43:00'),
       ('075','10:44:00'),
       ('076','10:45:00'),
       ('077','10:46:00'),
       ('078','10:47:00'),
       ('079','10:48:00'),
       ('080','10:49:00'),
       ('081','10:50:00'),
       ('082','10:51:00'),
       ('083','10:52:00'),
       ('084','10:53:00'),
       ('085','10:54:00'),
       ('086','10:55:00'),
       ('087','10:56:00'),
       ('088','10:57:00'),
       ('089','10:58:00'),
       ('090','10:59:00'),
       ('091','11:00:00'),
       ('092','11:01:00'),
       ('093','11:02:00'),
       ('094','11:03:00'),
       ('095','11:04:00'),
       ('096','11:05:00'),
       ('097','11:06:00'),
       ('098','11:07:00'),
       ('099','11:08:00'),
       ('100','11:09:00'),
       ('101','11:10:00'),
       ('102','11:11:00'),
       ('103','11:12:00'),
       ('104','11:13:00'),
       ('105','11:14:00'),
       ('106','11:15:00'),
       ('107','11:16:00'),
       ('108','11:17:00'),
       ('109','11:18:00'),
       ('110','11:19:00'),
       ('111','11:20:00'),
       ('112','11:21:00'),
       ('113','11:22:00'),
       ('114','11:23:00'),
       ('115','11:24:00'),
       ('116','11:25:00'),
       ('117','11:26:00'),
       ('118','11:27:00'),
       ('119','11:28:00'),
       ('120','11:29:00'),
       ('121','11:30:00'),
       ('122','11:31:00'),
       ('123','11:32:00'),
       ('124','11:33:00'),
       ('125','11:34:00'),
       ('126','11:35:00'),
       ('127','11:36:00'),
       ('128','11:37:00'),
       ('129','11:38:00'),
       ('130','11:39:00'),
       ('131','11:40:00'),
       ('132','11:41:00'),
       ('133','11:42:00'),
       ('134','11:43:00'),
       ('135','11:44:00'),
       ('136','11:45:00'),
       ('137','11:46:00'),
       ('138','11:47:00'),
       ('139','11:48:00'),
       ('140','11:49:00'),
       ('141','11:50:00'),
       ('142','11:51:00'),
       ('143','11:52:00'),
       ('144','11:53:00'),
       ('145','11:54:00'),
       ('146','11:55:00'),
       ('147','11:56:00'),
       ('148','11:57:00'),
       ('149','11:58:00'),
       ('150','11:59:00'),
       ('151','12:00:00'),
       ('152','12:01:00'),
       ('153','12:02:00'),
       ('154','12:03:00'),
       ('155','12:04:00'),
       ('156','12:05:00'),
       ('157','12:06:00'),
       ('158','12:07:00'),
       ('159','12:08:00'),
       ('160','12:09:00'),
       ('161','12:10:00'),
       ('162','12:11:00'),
       ('163','12:12:00'),
       ('164','12:13:00'),
       ('165','12:14:00'),
       ('166','12:15:00'),
       ('167','12:16:00'),
       ('168','12:17:00'),
       ('169','12:18:00'),
       ('170','12:19:00'),
       ('171','12:20:00'),
       ('172','12:21:00'),
       ('173','12:22:00'),
       ('174','12:23:00'),
       ('175','12:24:00'),
       ('176','12:25:00'),
       ('177','12:26:00'),
       ('178','12:27:00'),
       ('179','12:28:00'),
       ('180','12:29:00'),
       ('181','12:30:00'),
       ('182','12:31:00'),
       ('183','12:32:00'),
       ('184','12:33:00'),
       ('185','12:34:00'),
       ('186','12:35:00'),
       ('187','12:36:00'),
       ('188','12:37:00'),
       ('189','12:38:00'),
       ('190','12:39:00'),
       ('191','12:40:00'),
       ('192','12:41:00'),
       ('193','12:42:00'),
       ('194','12:43:00'),
       ('195','12:44:00'),
       ('196','12:45:00'),
       ('197','12:46:00'),
       ('198','12:47:00'),
       ('199','12:48:00'),
       ('200','12:49:00'),
       ('201','12:50:00'),
       ('202','12:51:00'),
       ('203','12:52:00'),
       ('204','12:53:00'),
       ('205','12:54:00'),
       ('206','12:55:00'),
       ('207','12:56:00'),
       ('208','12:57:00'),
       ('209','12:58:00'),
       ('210','12:59:00'),
       ('211','13:00:00'),
       ('212','13:01:00'),
       ('213','13:02:00'),
       ('214','13:03:00'),
       ('215','13:04:00'),
       ('216','13:05:00'),
       ('217','13:06:00'),
       ('218','13:07:00'),
       ('219','13:08:00'),
       ('220','13:09:00'),
       ('221','13:10:00'),
       ('222','13:11:00'),
       ('223','13:12:00'),
       ('224','13:13:00'),
       ('225','13:14:00'),
       ('226','13:15:00'),
       ('227','13:16:00'),
       ('228','13:17:00'),
       ('229','13:18:00'),
       ('230','13:19:00'),
       ('231','13:20:00'),
       ('232','13:21:00'),
       ('233','13:22:00'),
       ('234','13:23:00'),
       ('235','13:24:00'),
       ('236','13:25:00'),
       ('237','13:26:00'),
       ('238','13:27:00'),
       ('239','13:28:00'),
       ('240','13:29:00'),
       ('241','13:30:00'),
       ('242','13:31:00'),
       ('243','13:32:00'),
       ('244','13:33:00'),
       ('245','13:34:00'),
       ('246','13:35:00'),
       ('247','13:36:00'),
       ('248','13:37:00'),
       ('249','13:38:00'),
       ('250','13:39:00'),
       ('251','13:40:00'),
       ('252','13:41:00'),
       ('253','13:42:00'),
       ('254','13:43:00'),
       ('255','13:44:00'),
       ('256','13:45:00'),
       ('257','13:46:00'),
       ('258','13:47:00'),
       ('259','13:48:00'),
       ('260','13:49:00'),
       ('261','13:50:00'),
       ('262','13:51:00'),
       ('263','13:52:00'),
       ('264','13:53:00'),
       ('265','13:54:00'),
       ('266','13:55:00'),
       ('267','13:56:00'),
       ('268','13:57:00'),
       ('269','13:58:00'),
       ('270','13:59:00'),
       ('271','14:00:00'),
       ('272','14:01:00'),
       ('273','14:02:00'),
       ('274','14:03:00'),
       ('275','14:04:00'),
       ('276','14:05:00'),
       ('277','14:06:00'),
       ('278','14:07:00'),
       ('279','14:08:00'),
       ('280','14:09:00'),
       ('281','14:10:00'),
       ('282','14:11:00'),
       ('283','14:12:00'),
       ('284','14:13:00'),
       ('285','14:14:00'),
       ('286','14:15:00'),
       ('287','14:16:00'),
       ('288','14:17:00'),
       ('289','14:18:00'),
       ('290','14:19:00'),
       ('291','14:20:00'),
       ('292','14:21:00'),
       ('293','14:22:00'),
       ('294','14:23:00'),
       ('295','14:24:00'),
       ('296','14:25:00'),
       ('297','14:26:00'),
       ('298','14:27:00'),
       ('299','14:28:00'),
       ('300','14:29:00'),
       ('301','14:30:00'),
       ('302','14:31:00'),
       ('303','14:32:00'),
       ('304','14:33:00'),
       ('305','14:34:00'),
       ('306','14:35:00'),
       ('307','14:36:00'),
       ('308','14:37:00'),
       ('309','14:38:00'),
       ('310','14:39:00'),
       ('311','14:40:00'),
       ('312','14:41:00'),
       ('313','14:42:00'),
       ('314','14:43:00'),
       ('315','14:44:00'),
       ('316','14:45:00'),
       ('317','14:46:00'),
       ('318','14:47:00'),
       ('319','14:48:00'),
       ('320','14:49:00'),
       ('321','14:50:00'),
       ('322','14:51:00'),
       ('323','14:52:00'),
       ('324','14:53:00'),
       ('325','14:54:00'),
       ('326','14:55:00'),
       ('327','14:56:00'),
       ('328','14:57:00'),
       ('329','14:58:00'),
       ('330','14:59:00'),
       ('331','15:00:00'),
       ('332','15:01:00'),
       ('333','15:02:00'),
       ('334','15:03:00'),
       ('335','15:04:00'),
       ('336','15:05:00'),
       ('337','15:06:00'),
       ('338','15:07:00'),
       ('339','15:08:00'),
       ('340','15:09:00'),
       ('341','15:10:00'),
       ('342','15:11:00'),
       ('343','15:12:00'),
       ('344','15:13:00'),
       ('345','15:14:00'),
       ('346','15:15:00'),
       ('347','15:16:00'),
       ('348','15:17:00'),
       ('349','15:18:00'),
       ('350','15:19:00'),
       ('351','15:20:00'),
       ('352','15:21:00'),
       ('353','15:22:00'),
       ('354','15:23:00'),
       ('355','15:24:00'),
       ('356','15:25:00'),
       ('357','15:26:00'),
       ('358','15:27:00'),
       ('359','15:28:00'),
       ('360','15:29:00'),
       ('361','15:30:00'),
       ('362','15:31:00'),
       ('363','15:32:00'),
       ('364','15:33:00'),
       ('365','15:34:00'),
       ('366','15:35:00'),
       ('367','15:36:00'),
       ('368','15:37:00'),
       ('369','15:38:00'),
       ('370','15:39:00'),
       ('371','15:40:00'),
       ('372','15:41:00'),
       ('373','15:42:00'),
       ('374','15:43:00'),
       ('375','15:44:00'),
       ('376','15:45:00'),
       ('377','15:46:00'),
       ('378','15:47:00'),
       ('379','15:48:00'),
       ('380','15:49:00'),
       ('381','15:50:00'),
       ('382','15:51:00'),
       ('383','15:52:00'),
       ('384','15:53:00'),
       ('385','15:54:00'),
       ('386','15:55:00'),
       ('387','15:56:00'),
       ('388','15:57:00'),
       ('389','15:58:00'),
       ('390','15:59:00'),
       ('391','16:00:00')])
 
   #Commit your changes
   conn.commit()
  
   print() 
   print ("Record Updated successfully ")

except mysql.connector.Error as error :
    print("Failed to update record to database rollback: {}".format(error))
    #reverting changes because of exception
    conn.rollback()
finally:
    #closing database connection.
    if(conn.is_connected()):
        cursor_.close()
        conn.close()
        print("connection is closed")