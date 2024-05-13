from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
import botiga_db

app = FastAPI()

class ProductCreate(BaseModel):
    name: str
    description: str
    company: str
    price: float
    units: int
    subcategory_id: int

class ProductCreateResponse(BaseModel):
    msg: str

# Endpoint que mostra tots els productes
@app.get("/product")
def read():
    return botiga_db.productes_schema(botiga_db.read())

# Endpoint que mostra un producte segons id
@app.get("/product/{product_id}")
def read_product(product_id: int):
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
    
    # Cridem la funcio per guardar
    new_product = botiga_db.create_product(product_dict)
    if new_product:
        return {"msg": "S’ha afegit correctement"}
        
    else:
        return {"msg": "Producte no ha sigut creat"}
        