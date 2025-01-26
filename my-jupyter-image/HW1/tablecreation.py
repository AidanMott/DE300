import pymysql

db = pymysql.connect(host='am-de300.clw42ssy4ei4.us-east-1.rds.amazonaws.com',
                     port=3306,
                     user='admin',
                     passwd='OregonDucks24-',
                     database='HW1')

dbConn = db.cursor()

dropquery = """
            drop table if exists hwdata;
            """

createquery = """
              create table hwdata(TARGET int, CNT_CHILDREN int, CNT_FAM_MEMBERS int, CODE_GENDER varchar(48), FLAG_OWN_CAR varchar(48), FLAG_OWN_REALTY varchar(48), AMT_INCOME_TOTAL int, NAME_INCOME_TYPE varchar(255), AMT_CREDIT float, TOTALAREA_MODE float, HOUSETYPE_MODE varchar(255), EXT_SOURCE_1 float, DAYS_EMPLOYED int, DAYS_BIRTH int, EXT_SOURCE_2 float, EXT_SOURCE_3 float, NAME_EDUCATION_TYPE varchar(255), AMT_REQ_CREDIT_BUREAU_YEAR int, NAME_HOUSING_TYPE varchar(255), NAME_FAMILY_STATUS varchar(255));
              """
dbConn.execute(dropquery)
dbConn.execute(createquery)
