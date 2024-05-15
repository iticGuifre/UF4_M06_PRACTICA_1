from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
import botiga_db

app = FastAPI()

# PRIMERA PART --------------------------------------------------------------------------------

#Model per crear un nou producte
class ProductCreate(BaseModel):
    name: str
    description: str
    company: str
    price: float
    units: int
    subcategory_id: int


#Model per a la resposta al crear un producte
class ProductCreateResponse(BaseModel):
    msg: str


#Model per modificar un producte
class ProductUpdate(BaseModel):
    name: Union[str, None] = None
    description: Union[str, None] = None
    company: Union[str, None] = None
    price: Union[float, None] = None
    units: Union[int, None] = None
    subcategory_id: Union[int, None] = None


# Endpoint que mostra tots els productes
@app.get("/product")
def read():
    #Cridem la funció per veure les llistes de productes.
    return botiga_db.productes_schema(botiga_db.read())


# Endpoint que mostra un producte segons l'id
@app.get("/product/{product_id}")
def read_product(product_id: int):

    #Cridem la funció de llegir productes per id.
    product = botiga_db.read_product_by_id(product_id)
    if product:
        return product
    else:
        return {
        "msg": "Producte no trobat",
        "product_id": product_id
    }


# Endpoint que crea un producte
@app.post("/product", response_model=ProductCreateResponse)
def create_product(product_data: ProductCreate):
    product_dict = product_data.dict()
    
    # Cridem la funció per guardar.
    new_product = botiga_db.create_product(product_dict)
    if new_product:
        return {"msg": "S’ha afegit correctement"}
        
    else:
        return {"msg": "Producte no ha sigut creat"}  
      
# Endpoint que modifica un producte
@app.put("/product/{product_id}", response_model=ProductCreateResponse)
def update_product(product_id: int, product_data: ProductUpdate):
    product_dict = product_data.dict(exclude_unset=True)

    # Cridem la funció per modificar
    updated_product = botiga_db.update_product(product_id, product_dict)
    if updated_product:
        return {"msg": "S’ha modificat correctament"}
    else:
        return {"msg": "Producte no trobat"}

# Endpoint que esborra un producte
@app.delete("/product/{product_id}", response_model=ProductCreateResponse)
def delete_product(product_id: int):

    #Cridem la funció per borrar un producte
    deleted = botiga_db.delete_product(product_id)
    if deleted:
        return {"msg": "S’ha borrat correctament"}
    else:
        return {"msg": "Producte no trobat"}
    
# Endpoint que mostra tots els productes amb la següent informació:
# nom de la categoria, nom de la subcategoria, nom del producte, marca del producte i el preu.
@app.get("/productAll")
def read_all_products():

    #Cridem la funció per lleguir els productes d'una forma especifica.
    products = botiga_db.read_all_products()
    if products:
        return products
    else:
        return {"msg": "No s'han trobat productes"}
    

# SEGONA PART --------------------------------------------------------------------------------
# Endpoint que carrega tots els productes de un document csv
@app.post("/loadProducts")
def load_products():
    product_json = botiga_db.load_products()
    return product_json
        