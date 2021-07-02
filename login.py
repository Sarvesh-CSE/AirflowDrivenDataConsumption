from flask_sqlalchemy import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

def make_database():
    """
     Make the Postgres database and create the table
    """
    
    dbname = 'Users_info'
    username = 'postgres'
    password = 'riyansha'
    tablename = 'login_users'


    engine = create_engine('postgresql+psycopg2://%s:%s@localhost/%s'%(username,password,dbname))
    
    if not database_exists(engine.url):
            create_database(engine.url)

    conn = psycopg2.connect(database = dbname, user = username, password = password)        

    cur = conn.cursor()

    create_table = """CREATE TABLE IF NOT EXISTS %s
                 (
                    username      TEXT,    
                    email         TEXT,
                    password      TEXT
                 )    
                 """ % tablename

    cur.execute(create_table)
    conn.commit()
    conn.close()

if __name__ == "__main__":
         make_database()    
