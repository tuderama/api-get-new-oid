from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
import pathlib, json, re
import pandas as pd
import numpy as np

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _canon(s: object) -> str | None:
    """Normalisasi key agar match: buang spasi/zero-width, uppercase, hilangkan '.0'."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return None
    s = str(s).replace("\ufeff", "").replace("\u200b", "").strip()
    s = re.sub(r"\s+", "", s)
    if s.endswith(".0"):
        s = s[:-2]
    return s.upper()


@app.get("/api/v1/generate-new-oid-barang")
async def generate_new_oid():
    try:
        # Paths
        json_path = pathlib.Path("./old_to_new_oid_barang.json")
        csv_path = pathlib.Path("./aiso_template_barang.csv")
        out_path = pathlib.Path("./aiso_template_barang_fixed.csv")
        unmapped_txt = pathlib.Path("./aiso_template_barang_unmapped_keys.txt")

        # Cek file
        missing = [str(p) for p in (json_path, csv_path) if not p.exists()]
        if missing:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "code": 404,
                    "message": f"File tidak ditemukan: {', '.join(missing)}",
                },
            )

        # Load mapping JSON & normalisasi key/value
        with json_path.open("r", encoding="utf-8") as f:
            raw_map = json.load(f)
        oid_old2new = {_canon(k): _canon(v) for k, v in raw_map.items()}

        # Baca CSV, pastikan id_perkiraan sebagai string (hindari 123.0/1.23e+05)
        df = pd.read_csv(csv_path, dtype={"id_perkiraan": "string"})
        if "id_perkiraan" not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "code": 400,
                    "message": "Kolom 'id_perkiraan' tidak ditemukan di CSV.",
                },
            )

        # ---------- Mapping id_perkiraan ----------
        id_raw = df["id_perkiraan"]
        id_norm = id_raw.apply(_canon)
        mapped = id_norm.map(oid_old2new)

        # Kumpulkan key yang belum termap (untuk file bantuan)
        missing_mask = mapped.isna()
        unmapped_count = int(missing_mask.sum())
        unique_missing = id_norm[missing_mask].dropna().unique().tolist()
        if unmapped_count > 0:
            with unmapped_txt.open("w", encoding="utf-8") as w:
                w.write("\n".join(unique_missing))

        # Fallback: kalau tidak ketemu → pakai nilai normalisasi lama
        df["id_perkiraan"] = mapped.fillna(id_norm)

        # (opsional pengulangan mapping setelah fallback—mengikuti kode awal)
        id_norm2 = df["id_perkiraan"].apply(_canon)
        mapped2 = id_norm2.map(oid_old2new)
        df["id_perkiraan"] = mapped2.fillna(id_norm2)

        # ---------- Mapping id_parent dengan "keep zeros" ----------
        if "id_parent" in df.columns:
            parent_raw = df["id_parent"].astype("string")
            parent_norm = parent_raw.apply(_canon).fillna("0")
            parent_map = parent_norm.map(oid_old2new)
            # Jika parent_norm == "0" → tetap "0"; selain itu pakai hasil map, fallback "0"
            df["id_parent"] = np.where(parent_norm == "0", "0", parent_map.fillna("0"))

        # ---------- KONVERSI KE INTEGER ----------
        # Aman terhadap non-angka: coerce → NaN → fill 0 → int
        if "id_perkiraan" in df.columns:
            df["id_perkiraan"] = (
                pd.to_numeric(df["id_perkiraan"], errors="coerce").fillna(0).astype(int)
            )
        if "id_parent" in df.columns:
            df["id_parent"] = (
                pd.to_numeric(df["id_parent"], errors="coerce").fillna(0).astype(int)
            )

        # Simpan hasil ke CSV
        df.to_csv(out_path, index=False)

        # Bersihkan nilai ekstrem untuk jaga-jaga (harusnya sudah aman)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        data_rows = jsonable_encoder(df.to_dict(orient="records"))

        # Respons minimal (tanpa debug)
        payload = {
            "status": "success",
            "code": 200,
            "data": data_rows,
        }
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "code": 500,
                "message": f"Gagal memproses: {e}",
            },
        )


@app.get("/api/v1/generate-new-oid-retail")
async def generate_new_oid():
    try:
        # Paths
        json_path = pathlib.Path("./old_to_new_oid_retail.json")
        csv_path = pathlib.Path("./aiso_template_barang.csv")
        out_path = pathlib.Path("./aiso_template_barang_fixed.csv")
        unmapped_txt = pathlib.Path("./aiso_template_barang_unmapped_keys.txt")

        # Cek file
        missing = [str(p) for p in (json_path, csv_path) if not p.exists()]
        if missing:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "code": 404,
                    "message": f"File tidak ditemukan: {', '.join(missing)}",
                },
            )

        # Load mapping JSON & normalisasi key/value
        with json_path.open("r", encoding="utf-8") as f:
            raw_map = json.load(f)
        oid_old2new = {_canon(k): _canon(v) for k, v in raw_map.items()}

        # Baca CSV, pastikan id_perkiraan sebagai string (hindari 123.0/1.23e+05)
        df = pd.read_csv(csv_path, dtype={"id_perkiraan": "string"})
        if "id_perkiraan" not in df.columns:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "code": 400,
                    "message": "Kolom 'id_perkiraan' tidak ditemukan di CSV.",
                },
            )

        # ---------- Mapping id_perkiraan ----------
        id_raw = df["id_perkiraan"]
        id_norm = id_raw.apply(_canon)
        mapped = id_norm.map(oid_old2new)

        # Kumpulkan key yang belum termap (untuk file bantuan)
        missing_mask = mapped.isna()
        unmapped_count = int(missing_mask.sum())
        unique_missing = id_norm[missing_mask].dropna().unique().tolist()
        if unmapped_count > 0:
            with unmapped_txt.open("w", encoding="utf-8") as w:
                w.write("\n".join(unique_missing))

        # Fallback: kalau tidak ketemu → pakai nilai normalisasi lama
        df["id_perkiraan"] = mapped.fillna(id_norm)

        # (opsional pengulangan mapping setelah fallback—mengikuti kode awal)
        id_norm2 = df["id_perkiraan"].apply(_canon)
        mapped2 = id_norm2.map(oid_old2new)
        df["id_perkiraan"] = mapped2.fillna(id_norm2)

        # ---------- Mapping id_parent dengan "keep zeros" ----------
        if "id_parent" in df.columns:
            parent_raw = df["id_parent"].astype("string")
            parent_norm = parent_raw.apply(_canon).fillna("0")
            parent_map = parent_norm.map(oid_old2new)
            # Jika parent_norm == "0" → tetap "0"; selain itu pakai hasil map, fallback "0"
            df["id_parent"] = np.where(parent_norm == "0", "0", parent_map.fillna("0"))

        # ---------- KONVERSI KE INTEGER ----------
        # Aman terhadap non-angka: coerce → NaN → fill 0 → int
        if "id_perkiraan" in df.columns:
            df["id_perkiraan"] = (
                pd.to_numeric(df["id_perkiraan"], errors="coerce").fillna(0).astype(int)
            )
        if "id_parent" in df.columns:
            df["id_parent"] = (
                pd.to_numeric(df["id_parent"], errors="coerce").fillna(0).astype(int)
            )

        # Simpan hasil ke CSV
        df.to_csv(out_path, index=False)

        # Bersihkan nilai ekstrem untuk jaga-jaga (harusnya sudah aman)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        data_rows = jsonable_encoder(df.to_dict(orient="records"))

        # Respons minimal (tanpa debug)
        payload = {
            "status": "success",
            "code": 200,
            "data": data_rows,
        }
        return JSONResponse(status_code=200, content=payload)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "code": 500,
                "message": f"Gagal memproses: {e}",
            },
        )
