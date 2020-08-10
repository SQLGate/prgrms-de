import psycopg2
import requests

# Redshift connection 함수
def get_Redshift_connection():
    host = "connectionhost"
    redshift_user = "user"
    redshift_pass = "pass"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(url):
    f = requests.get(link)
    return (f.text)


def transform(text):
    lines = text.split("\n")
    return lines

def load(text):
    cur = get_Redshift_connection()
    sqls = []
    allsqls = []
    execnt = 0
    tablename = "ysyang.name_gender"

    for idx,r in enumerate(text) :
      if idx > 0 :
        if r != '':
            (name, gender) = r.split(",")            
            sql = "INSERT INTO {tablename} VALUES ('{name}', '{gender}',getdate());".format(tablename= tablename, name=name, gender=gender)
            sqls.append(sql)
            execnt += 1
            # print("{}.{}".format(idx,sql))

    allsqls.append('begin;')
    allsqls.append('delete from {};'.format(tablename))
    allsqls.extend(sqls)
    allsqls.append('commit;')
    allsqls.append('end;')

    executesql = "\n\r".join(allsqls)
    try:
      cur.execute(executesql)
      print("Successfully executed {} queries.".format(execnt))
      print(executesql)
    except Exception as e: 
      print("Error : {}".format(e))
      print("Rollback complete.")
    cur.close()    

if __name__ == "__main__":    
  link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
  data = extract(link)
  lines = transform(data)
  load(lines)