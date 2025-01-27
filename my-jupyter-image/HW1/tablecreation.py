import pymysql
import csv

dbConn = pymysql.connect(host='am-de300.clw42ssy4ei4.us-east-1.rds.amazonaws.com',
                     port=3306,
                     user='admin',
                     passwd='OregonDucks24-',
                     database='HW1')

dbCursor = dbConn.cursor()

dropquery = """
            drop table if exists hwdata;
            """

createquery = """
              create table hwdata(TARGET int, CNT_CHILDREN int, CNT_FAM_MEMBERS int, CODE_GENDER varchar(48), FLAG_OWN_CAR varchar(48), FLAG_OWN_REALTY varchar(48), AMT_INCOME_TOTAL int, NAME_INCOME_TYPE varchar(255), AMT_CREDIT float, TOTALAREA_MODE float, HOUSETYPE_MODE varchar(255), EXT_SOURCE_1 float, DAYS_EMPLOYED int, DAYS_BIRTH int, EXT_SOURCE_2 float, EXT_SOURCE_3 float, NAME_EDUCATION_TYPE varchar(255), AMT_REQ_CREDIT_BUREAU_YEAR int, NAME_HOUSING_TYPE varchar(255), NAME_FAMILY_STATUS varchar(255));
              """
dbCursor.execute(dropquery)
dbCursor.execute(createquery)

with open("my-jupyter-image\\HW1\\data.csv", "r") as data:
    datareader = csv.reader(data)
    print(next(datareader))
    insertlist = []
    sql = """
          insert into hwdata(TARGET, CNT_CHILDREN, CNT_FAM_MEMBERS, CODE_GENDER, FLAG_OWN_CAR, FLAG_OWN_REALTY, AMT_INCOME_TOTAL, NAME_INCOME_TYPE, AMT_CREDIT, TOTALAREA_MODE, HOUSETYPE_MODE, EXT_SOURCE_1, DAYS_EMPLOYED, DAYS_BIRTH, EXT_SOURCE_2, EXT_SOURCE_3, NAME_EDUCATION_TYPE, AMT_REQ_CREDIT_BUREAU_YEAR, NAME_HOUSING_TYPE, NAME_FAMILY_STATUS)
          values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
          """
    for row in datareader:
        insertlist.append(row)
    dbCursor.executemany(sql, insertlist)
    dbCursor.close()
    dbConn.commit()
    dbConn.close()