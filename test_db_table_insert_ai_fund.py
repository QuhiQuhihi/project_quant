import os
import sys
import datetime
#import click
from dotenv import load_dotenv

import traceback
import numpy as np
import pandas as pd

import sqlalchemy
from mail_testbed import sendmail

from config import ENV_PATH
from config import init_config

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from models.df import KfrFndTs

from config import Configuration

# https://docs.sqlalchemy.org/en/14/core/dml.html#sqlalchemy.sql.expression.Insert.values
# https://www.fun-coding.org/mysql_advanced2.html

def create_db_engine(config: Configuration) -> Engine:
    url = 'postgresql://{user}:{password}@{host}:{port}/{db}'
    url = url.format(
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        db=config.dbname,
    )

    return create_engine(url, echo=config.echo)


def create_db_session(engine: Engine):
    session = sessionmaker(bind=engine, autocommit=False)
    return session


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

def db_conn_test():

    nas_dir = os.path.join('/nas', 'DnCoPlatformDev',
                           '조직별', '포트폴리오개발실','김다함')
    kyobo_dir = os.path.join('/nas', 'DnCoPlatformDev', 'ISAAC_V_2_research','IUG','DB','fnd','kyobo','temp')

    config = init_config()
    db_engine = create_db_engine(config=config)  # DB 엔진 정의
    db_engine.connect()  # DB 커넥션 생성
    session = create_db_session(engine=db_engine)  # DB 세션 생성

    ### below code is to query simple result from db table

    mother = pd.read_excel(os.path.join(kyobo_dir, '기준가_v2.xlsx'))
    # print(mother)


    mother.loc[:,'date'] = mother.loc[:,'date'].astype(str)
    mother.loc[:,'fund_cd'] = mother.loc[:,'fund_cd'].astype(str)
    mother.loc[:,'px'] = mother.loc[:,'px'].astype(float)
    mother.loc[:,'nav'] = mother.loc[:,'nav'].astype(float)
    mother.loc[:,'rtn'] = mother.loc[:,'rtn'].astype(float)
    mother.loc[:,'bm_rtn'] = mother.loc[:,'bm_rtn'].astype(float)
    mother.loc[:,'file_date'] = mother.loc[:,'file_date'].astype(str)
    mother.loc[:, 'created_at'] = pd.to_datetime("now")
    mother.loc[:, 'updated_at'] = pd.to_datetime("now")

    print(mother)
    input("================")
    # print(mother)

    try:
        session = session()  # SQLAlchemy 에서 사용할 세션 정의
        mother.to_sql('kfr_fund_ts', con=db_engine, schema='public', if_exists='append', index=False,
                   dtype={
                       'date': sqlalchemy.VARCHAR(100),
                       'fund_cd': sqlalchemy.VARCHAR(100),
                       'px': sqlalchemy.FLOAT(),
                       'nav': sqlalchemy.FLOAT(),
                       'rtn': sqlalchemy.FLOAT(),
                       'bm_rtn': sqlalchemy.FLOAT(),
                       'file_date': sqlalchemy.VARCHAR(100),
                       'created_at':sqlalchemy.DATETIME(),
                       'updated_at': sqlalchemy.DATETIME()
                   })
    except Exception as error:
        print("db error")
        # raise error
        to_emails = ['dh.kim@dco.com']
        title = '[ERROR] US_to_KFR_Kyobo test - '
        msg = '{}'.format(traceback.format_exc())
        sendmail(title, msg, to_email=to_emails)
        return
    finally:
        session.close()  # DB 세션 닫기


if __name__ == '__main__':
    db_conn_test()

