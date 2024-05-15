from client import db_client
import datetime

# ---------------------------------------------- PRIMERA PART ----------------------------------------------

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


# ---------------------------------------------- SEGONA PART ----------------------------------------------

# Comprova que la categoria existeix per el nom
def category_exists(name: str) -> bool:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT category_id FROM category WHERE name = %s", (name,))
        result = cur.fetchone()
        return result is not None
    except Exception as e:
        print(f"Error comprobant que existeix la categoria: {e}")
        return False
    finally:
        conn.close()

# Obte el id de categoria per el nom
def get_category_id(name: str) -> int:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT category_id FROM category WHERE name = %s", (name,))
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error obtenint id de categoria: {e}")
        return None
    finally:
        conn.close()

# Crea categoria per nom
def create_category(name: str) -> int:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO category (name) 
            VALUES (%s)
            """, (name,))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        print(f"Error creant categoria: {e}")
        return None
    finally:
        conn.close()

# Actualitza la categoria per id, canviant el nom
def update_category(category_id: int, name: str):
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("""
            UPDATE category 
            SET name = %s
            WHERE category_id = %s
            """, (name, category_id))
        conn.commit()
    except Exception as e:
        print(f"Error actualitzant categoria: {e}")
    finally:
        conn.close()

# Comprova que la subcategoria existeix per el nom
def subcategory_exists_by_name(subcategory_name: str) -> bool:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT subcategory_id FROM subcategory WHERE name = %s", (subcategory_name,))
        result = cur.fetchone()
        return result is not None
    except Exception as e:
        print(f"Error comprobant que la subcategoria existeix: {e}")
        return False
    finally:
        conn.close()

# Obte el id de subcategoria per el nom
def get_subcategory_id_by_name(subcategory_name: str) -> int:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT subcategory_id FROM subcategory WHERE name = %s", (subcategory_name,))
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error obtenint id de subcategoria: {e}")
        return None
    finally:
        conn.close()

# Obte el id de subcategoria per el nom
def get_subcategory_id(name: str) -> int:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT subcategory_id FROM subcategory WHERE name = %s", (name,))
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error obtenint id de subcategoria: {e}")
        return None
    finally:
        conn.close()

# Crea la subcategoria per id i nom
def create_subcategory(name: str, category_id: int) -> int:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("INSERT INTO subcategory (name, category_id) VALUES (%s, %s)", (name, category_id))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        print(f"Error creant subcategoria: {e}")
        return None
    finally:
        conn.close()

# Actualitza subcategoria per id canviant el nom
def update_subcategory(subcategory_id: int, name: str):
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("UPDATE subcategory SET name = %s WHERE subcategory_id = %s", (name, subcategory_id))
        conn.commit()
    except Exception as e:
        print(f"Error actualitzant subcategoria: {e}")
    finally:
        conn.close()
    
# Comproba si el producte existeix
def product_exists(product_id: int) -> bool:
    try:
        conn = db_client()
        cur = conn.cursor()
        cur.execute("SELECT product_id FROM product WHERE product_id = %s", (product_id,))
        result = cur.fetchone()
        return result is not None
    except Exception as e:
        print(f"Error al comprobar que el producte existeix: {e}")
        return False
    finally:
        conn.close()

# Actualitza el producte (la creacio esta al principi)
def update_product(product_id: int, product_data: dict):
    try:
        conn = db_client()
        cur = conn.cursor()
        now = datetime.datetime.now()
        product_data['updated_at'] = now
        cur.execute("""
            UPDATE product
            SET name = %(name)s, description = %(description)s, company = %(company)s,
                price = %(price)s, units = %(units)s, subcategory_id = %(subcategory_id)s, updated_at = %(updated_at)s
            WHERE product_id = %(product_id)s
            """, {**product_data, 'product_id': product_id})
        conn.commit()
    except Exception as e:
        print(f"Error actualitzant producte: {e}")
    finally:
        conn.close()

# Funcio que s'encarrega de carregar els productes de un fitxer csv a la base de dades.
def load_products():
    product_list = []

    # S'obre el fitxer i es guarden en memoria tots els productes amb un format de diccionari
    with open("llista_productes.csv") as productes_csv:
        productes = productes_csv.readlines()
        for producte in productes:
            producte_split = producte.split(',')
            producte_json = {
                'id': producte_split[0],
                'category_name': producte_split[1],
                'subcategory_id': producte_split[2],
                'subcategory_name': producte_split[3],
                'product_id': producte_split[4],
                'product_name': producte_split[5],
                'product_description': producte_split[6],
                'company': producte_split[7],
                'product_price': producte_split[8],
                'product_units': producte_split[9][:-1]
            }
            product_list.append(producte_json)
        productes_csv.close()

    # Un cop tenim els productes, ens desfem de la primera linia
    product_list.pop(0)

    # Aquest bucle es dedica a consultar si la categoria, subcategoria i producte existeixen,
    # en cas de no existir es persisteixen i en cas d'existir s'actualitzen
    for product in product_list:
        # Agafem dades d'utilitat del producte
        category_name = product['category_name']
        subcategory_name = product['subcategory_name']
        subcategory_id = product['subcategory_id']
        product_id = product['product_id']

        # Ens encarreguem de comprobar si existeix o no la categoria
        if category_exists(category_name):
            # Si la categoria existeix, actualitzem el nom
            category_id = get_category_id(category_name)
            update_category(category_id, category_name)
        else:
            # Si la categoria no existeix, la creem
            category_id = create_category(category_name)

        # Ens encarreguem de comprobar si existeix o no la subcategoria
        if subcategory_exists_by_name(subcategory_name):
            # Si la subcategoria existeix, actualitzem el nom
            subcategory_id = get_subcategory_id_by_name(subcategory_name)
            update_subcategory(subcategory_id, subcategory_name)
        else:
            # Si la subcategoria no existeix, la creem
            subcategory_id = create_subcategory(subcategory_name, category_id)
        
        # Fem un diccionari exclusiu de producte que ens servira per pasar les dades tant per crear com per actualitzar
        product_data = {
            'name': product['product_name'],
            'description': product['product_description'],
            'company': product['company'],
            'price': product['product_price'],
            'units': product['product_units'],
            'subcategory_id': subcategory_id
        }

        # Ens encarreguem de comprobar si existeix o no el producte
        if product_exists(product_id):
            # Si existeix, actualitzem
            update_product(product_id, product_data)
        else:
            # Si no existeix, creem
            create_product(product_data)
    return {"msg": "Els productes s'han carregat correctament"}


