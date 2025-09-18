from fastapi import FastAPI, UploadFile, File, HTTPException
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

@app.post("/generate-new-oid")
async def generate_new_oid(file: UploadFile | None = File(None)):
    if file is None:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "code": 400,
                    "message": "Tidak ada file yang diupload",
                },
        )
    # Validasi format file
    if not file.filename.lower().endswith(".csv"):
        return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "code": 400,
                    "message": "File wajib csv",
                },
        )

    # Path untuk simpan file upload
    save_path = pathlib.Path("./aiso_template.csv")

    try:
        # Simpan file user sebagai aiso_template.csv
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Jalankan proses mapping
        json_path = pathlib.Path("./new_oid_RAG.json")
        csv_path  = pathlib.Path("./aiso_template.csv")
        out_path  = pathlib.Path("./aiso_template_fixed.csv")

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

        with json_path.open("r", encoding="utf-8") as f:
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

        # mapping id_perkiraan
        id_str = df["id_perkiraan"].astype(str).str.strip()
        mapped_oid = id_str.map(oid_old2new)
        df["id_perkiraan"] = mapped_oid.fillna(df["id_perkiraan"].astype(str))

        # mapping id_parent
        parent_str = df["id_parent"].astype(str).str.strip()
        parent_mapped = parent_str.map(oid_old2new).fillna("0")
        df["id_parent"] = parent_mapped

        # jika id_parent == id_perkiraan → jadikan 0
        mask_self = df["id_parent"].astype(str) == df["id_perkiraan"].astype(str)
        df.loc[mask_self, "id_parent"] = "0"

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
