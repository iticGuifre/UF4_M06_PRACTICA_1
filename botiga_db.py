from client import db_client

# Llegeix tots els productes de la taula (product)
def read():
    try:
        #Proba la conexió a la BD.
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

# Llegeix un producte per id
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

# Crea un nou producte amb les dades proporcionades
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

# Modifica un producte existen amb les noves dades proporcionades
def update_product(product_id: int, product_data: dict):
    try:
        conn = db_client()
        cur = conn.cursor()
        set_clause = ", ".join(f"{key} = %({key})s" for key in product_data.keys())
        product_data["product_id"] = product_id
        cur.execute(f"""
            UPDATE product
            SET {set_clause}, updated_at = CURRENT_TIMESTAMP
            WHERE product_id = %(product_id)s
            """, product_data)
        conn.commit()
        
        cur.execute("SELECT * FROM product WHERE product_id = %s", (product_id,))
        result = cur.fetchone()
        if result:
            return producte_schema(result)
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    finally:
        conn.close()

# Elimina un producte per id
def delete_product(product_id: int):
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("DELETE FROM product WHERE product_id = %s", (product_id,))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    finally:
        conn.close()

# Mostra tots els productes de forma específica. 
def read_all_products():
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("""
            SELECT category.name as category_name, subcategory.name as subcategory_name,
                   product.name as product_name, product.company, product.price
            FROM product
            JOIN subcategory ON product.subcategory_id = subcategory.subcategory_id
            JOIN category ON subcategory.category_id = category.category_id
            """)
        result = cur.fetchall()
    except Exception as e:
        return {"status": -1, "message": f"Error de connexió:{e}"}
    finally:
        conn.close()
    return [{"category_name": row[0], "subcategory_name": row[1], "product_name": row[2], "company": row[3], "price": row[4]} for row in result]