from client import db_client

# Llegeix tots els productes
def read():
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("select * from product")

        result = cur.fetchall()
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    
    finally:
        conn.close()
    
    return result

# Transforma el producte en diccionari
def producte_schema(producte) -> dict:
    return {
        "product_id": producte[0],
        "name": producte[1],
        "description": producte[2],
        "company": producte[3],
        "price": producte[4],
        "units": producte[5],
        "subcategory_id": producte[6],
        "created_at": producte[7],
        "updated_at": producte[8]
    }

# Mostra llista de productes
def productes_schema(productes) -> dict:
    return [producte_schema(producte) for producte in productes]

# Llgeix productes per id
def read_product_by_id(product_id: int):
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT * FROM product WHERE product_id = %s", (product_id,))
        result = cur.fetchone()
        if result:
            return producte_schema(result)
        
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    finally:
        conn.close()

# Crea un producte
def create_product(product_data: dict):
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO product (name, description, company, price, units, subcategory_id) 
            VALUES (%(name)s, %(description)s, %(company)s, %(price)s, %(units)s, %(subcategory_id)s)
            """, product_data)
        conn.commit()
        
        # Agafem l'ultim insertat per retornarlo
        cur.execute("SELECT * FROM product WHERE product_id = LAST_INSERT_ID()")
        result = cur.fetchone()
        if result:
            return producte_schema(result)

    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    finally:
        conn.close()