import mysql.connector

def db_client():
    try:
        dbname = "botiga"
        user = "dam_app"
        password = "1234"
        host = "localhost"
        port = "3306"

        return mysql.connector.connect(
            host = host,
            port = port,
            user = user,
            password = password,
            database = dbname
        )
    
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}