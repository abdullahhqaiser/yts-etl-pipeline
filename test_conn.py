import pyodbc

conn = pyodbc.connect(
            'Driver={SQL Server};'
            'Server=yts-server.database.windows.net;'
            'Database=yts_warehouse;'
            'Trusted_Connection=no;'
            'UID=incomingsign;'
            'PWD=Amrina2000;'
            'autocommit=False')
print(conn)