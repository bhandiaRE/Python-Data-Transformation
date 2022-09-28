import json 
import requests
import pyodbc

url = 'https://locator.sasol.com/api/station.json'

r = requests.get(url)
server = 'Server\SQLEXPRESS'
database = 'Sasol'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                      SERVER=' + server +'; \
                      DATABASE=' + database +';\
                      Trusted_Connection=yes;')
cnxn.autocommit = True


data = json.loads(r.text)
for i in range(0,3):
    name = (data['stations'][i]['station_address'])
    
    json_string = json.dumps(name)
    print(json_string)
    try:
        cursor=cnxn.cursor()
        cursor.execute('INSERT INTO [Sasol].[dbo].[SasolGarage] ("Sasol Garage Address")\
                        VALUES(?);',json_string)
        print('Inserted')
        
    except pyodbc.Error as err:
        print('Error !!!!! %s' % err)
    except:
        print('Something else Failed')
        
cursor.close()
cnxn.close()
