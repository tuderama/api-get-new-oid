from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pathlib, shutil, csv, json
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],  
    allow_headers=["*"],
)

@app.get("/api/v1/generate-new-oid-barang")
async def generate_new_oid():
    try:
    
        # Jalankan proses mapping
        json_path = pathlib.Path("./old_to_new_oid_barang.json")
        csv_path  = pathlib.Path("./aiso_template_barang.csv")
        out_path  = pathlib.Path("./aiso_template_barang_fixed.csv")

        missing = []
        if not json_path.exists():
            missing.append(str(json_path))
        if not csv_path.exists():
            missing.append(str(csv_path))
        if missing:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "code": 404,
                    "message": f"File tidak ditemukan: {', '.join(missing)}",
                },
            )

        with json_path.open("r") as f:
            oid_old2new = json.load(f)

        df = pd.read_csv(csv_path)
        if "id_perkiraan" not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "code": 400,
                    "message": "Kolom 'id_perkiraan' tidak ditemukan di CSV.",
                },
            )

        df["OID"] = df['id_perkiraan'].map(oid_old2new)
        df['id_perkiraan'] = df['OID']
        df = df.drop(columns=['OID'])


        # simpan hasil fixed
        df.to_csv(out_path, index=False)

        data_rows = df.to_dict(orient="records")

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "code": 200,
                "data": data_rows
            },
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "code": 500,
                "message": f"Gagal memproses: {e}",
            },
        )

