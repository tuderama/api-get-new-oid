from fastapi import FastAPI
from fastapi.responses import JSONResponse
import json
import pathlib

app = FastAPI()

@app.get("/generate-new-oid")
async def generate_new_oid():
    file_path = pathlib.Path("./oidbaru2coa_perusahaan-barang.json")

    if not file_path.exists():
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "code": 404,
                "message": "File tidak ditemukan",
            },
        )

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "code": 200,
                "data": data,
            },
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "code": 500,
                "message": f"Gagal membaca file: {e}",
            },
        )
