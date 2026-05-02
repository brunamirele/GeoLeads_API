import duckdb
import math
import numpy as np

# 🔐 CONFIGURAÇÕES DO R2 (PREENCHER!)
S3_ACCESS_KEY = "84a0e359248d26886fedea13d97c1a0d"
S3_SECRET_KEY = "098f680a704073c507d5928efb655f9ce14d82808814758caa3ee4d19658b214"
S3_ENDPOINT = "b45d1e7d377211530520ec305557fff8.r2.cloudflarestorage.com"
S3_BUCKET = "dados-rf"

# cria conexão global (melhor performance)
con = duckdb.connect()

# 🔌 habilita leitura de arquivos remotos
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# 🔐 configura acesso ao R2 (S3)
con.execute(f"""
SET s3_region='auto';
SET s3_access_key_id='{S3_ACCESS_KEY}';
SET s3_secret_access_key='{S3_SECRET_KEY}';
SET s3_endpoint='{S3_ENDPOINT}';
""")

def buscar_empresas(lat, lon, raio_km, cnae=None, limite=100):

    delta_lat = raio_km / 111
    delta_lon = raio_km / (111 * math.cos(math.radians(lat)))

    lat_min = lat - delta_lat
    lat_max = lat + delta_lat
    lon_min = lon - delta_lon
    lon_max = lon + delta_lon

    # 📍 caminho agora no R2 (S3)
    data_path = f"s3://{S3_BUCKET}/*.parquet"

    # 🧠 filtro opcional de CNAE
    filtro_cnae = ""
    if cnae:
        filtro_cnae = f"AND cnae_principal LIKE '{cnae}%'"

    query = f"""
    SELECT *
        FROM (
            SELECT 
                cnpj_completo, 
                cnpj_basico, 
                razao_social, 
                porte_grupo,
                nome_fantasia, 
                porte_empresa, 
                descricao_natureza,
                cnae_principal, 
                uf, 
                nome_municipio, 
                logradouro, 
                numero,
                endereco, 
                cep, 
                bairro, 
                telefone_1, 
                telefone_2, 
                email,
                segmento, 
                latitude, 
                longitude,

                6371 * acos(
                    cos(radians({lat})) *
                    cos(radians(latitude)) *
                    cos(radians(longitude) - radians({lon})) +
                    sin(radians({lat})) *
                    sin(radians(latitude))
                ) AS distancia

            FROM read_parquet('s3://{S3_BUCKET}/*.parquet')

            WHERE latitude BETWEEN {lat_min} AND {lat_max}
            AND longitude BETWEEN {lon_min} AND {lon_max}
        ) t

        WHERE distancia <= {raio_km}
        ORDER BY distancia
        LIMIT {limite}
        """

    df = con.execute(query).df()

    # 🔥 corrige problema de JSON (NaN / infinito)
    df = df.replace([np.nan, np.inf, -np.inf], None)

    return df.to_dict(orient="records")

'''import duckdb
import math
import numpy as np
import os

# 🔐 CONFIGURAÇÕES DO R2 (PREENCHER!)
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")

# cria conexão global (melhor performance)
con = duckdb.connect()

# 🔌 habilita leitura de arquivos remotos
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# 🔐 configura acesso ao R2 (S3)
con.execute(f"""
SET s3_region='auto';
SET s3_access_key_id='{S3_ACCESS_KEY}';
SET s3_secret_access_key='{S3_SECRET_KEY}';
SET s3_endpoint='{S3_ENDPOINT}';
""")

def buscar_empresas(lat, lon, raio_km, cnae=None, limite=100):

    delta_lat = raio_km / 111
    delta_lon = raio_km / (111 * math.cos(math.radians(lat)))

    lat_min = lat - delta_lat
    lat_max = lat + delta_lat
    lon_min = lon - delta_lon
    lon_max = lon + delta_lon

    # 📍 caminho agora no R2 (S3)
    data_path = f"s3://{S3_BUCKET}/*.parquet"

    # 🧠 filtro opcional de CNAE
    filtro_cnae = ""
    if cnae:
        filtro_cnae = f"AND cnae_principal LIKE '{cnae}%'"

    query = f"""
    SELECT *
        FROM (
            SELECT 
                cnpj_completo, 
                cnpj_basico, 
                razao_social, 
                porte_grupo,
                nome_fantasia, 
                porte_empresa, 
                descricao_natureza,
                cnae_principal, 
                uf, 
                nome_municipio, 
                logradouro, 
                numero,
                endereco, 
                cep, 
                bairro, 
                telefone_1, 
                telefone_2, 
                email,
                segmento, 
                latitude, 
                longitude,

                6371 * acos(
                    cos(radians({lat})) *
                    cos(radians(latitude)) *
                    cos(radians(longitude) - radians({lon})) +
                    sin(radians({lat})) *
                    sin(radians(latitude))
                ) AS distancia

            FROM read_parquet('s3://{S3_BUCKET}/*.parquet')

            WHERE latitude BETWEEN {lat_min} AND {lat_max}
            AND longitude BETWEEN {lon_min} AND {lon_max}
        ) t

        WHERE distancia <= {raio_km}
        ORDER BY distancia
        LIMIT {limite}
        """

    df = con.execute(query).df()

    # 🔥 corrige problema de JSON (NaN / infinito)
    df = df.replace([np.nan, np.inf, -np.inf], None)

    return df.to_dict(orient="records")
'''
'''import duckdb
import math
import os
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "dataset_enriquecido", "*.parquet")
DATA_PATH = DATA_PATH.replace("\\", "/")

def buscar_empresas(lat, lon, raio_km, cnae=None, limite=100):

    delta_lat = raio_km / 111
    delta_lon = raio_km / (111 * math.cos(math.radians(lat)))

    lat_min = lat - delta_lat
    lat_max = lat + delta_lat
    lon_min = lon - delta_lon
    lon_max = lon + delta_lon

    query = f"""
    SELECT 
        cnpj_completo, 
        cnpj_basico, 
        razao_social, 
        porte_grupo,
        nome_fantasia, 
        porte_empresa, 
        descricao_natureza,
        cnae_principal, 
        uf, 
        nome_municipio, 
        logradouro, 
        numero,
        endereco, 
        cep, 
        bairro, 
        telefone_1, 
        telefone_2, 
        email,
        segmento, 
        latitude, 
        longitude,

        6371 * acos(
            cos(radians({lat})) *
            cos(radians(latitude)) *
            cos(radians(longitude) - radians({lon})) +
            sin(radians({lat})) *
            sin(radians(latitude))
        ) AS distancia

    FROM '{DATA_PATH}'

    WHERE latitude BETWEEN {lat_min} AND {lat_max}
    AND longitude BETWEEN {lon_min} AND {lon_max}
    AND (
        6371 * acos(
            cos(radians({lat})) *
            cos(radians(latitude)) *
            cos(radians(longitude) - radians({lon})) +
            sin(radians({lat})) *
            sin(radians(latitude))
        )
    ) <= {raio_km}

    ORDER BY distancia
    LIMIT {limite}
    """

    df = duckdb.query(query).to_df()

    # 🔥 substitui NaN / inf corretamente
    df = df.replace([np.nan, np.inf, -np.inf], None)

    return df.to_dict(orient="records")'''
