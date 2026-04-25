from fastapi import FastAPI
from api.query_engine import buscar_empresas
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://geoleads-943417124528.us-west1.run.app/"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"status": "API rodando"}

@app.get("/buscar")
def buscar(lat: float, lon: float, raio: float, cnae: str = None):
    return buscar_empresas(lat, lon, raio, cnae)
