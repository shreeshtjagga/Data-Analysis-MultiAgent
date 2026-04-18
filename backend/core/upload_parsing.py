import csv
import io
import zipfile
from typing import Optional

import pandas as pd
from fastapi import HTTPException


def looks_like_csv(file_bytes: bytes) -> bool:
    sample = file_bytes[:16384]
    if not sample:
        return False

    text: Optional[str] = None
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            text = sample.decode(encoding)
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        return False

    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 1:
        return False

    sniff_sample = "\n".join(lines[:10])
    try:
        csv.Sniffer().sniff(sniff_sample)
        return True
    except csv.Error:
        return any(delim in lines[0] for delim in (",", ";", "\t", "|"))


def validate_upload_magic(parsed_ext: str, file_bytes: bytes) -> None:
    if parsed_ext == "csv":
        if not looks_like_csv(file_bytes):
            raise HTTPException(status_code=400, detail="Invalid CSV content")
        return

    if parsed_ext == "xlsx":
        if not zipfile.is_zipfile(io.BytesIO(file_bytes)):
            raise HTTPException(status_code=400, detail="Invalid XLSX file signature")
        return

    if parsed_ext == "xls":
        if not file_bytes.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            raise HTTPException(status_code=400, detail="Invalid XLS file signature")
        return

    raise HTTPException(status_code=400, detail="Unsupported file type")


def detect_csv_delimiter(file_bytes: bytes) -> Optional[str]:
    sample = file_bytes[:16384]
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = sample.decode(encoding)
            break
        except UnicodeDecodeError:
            text = None
    if not text:
        return None

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    sniff_sample = "\n".join(lines[:20])
    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return None


def read_csv_with_fallback(
    file_bytes: bytes,
    max_analyze_rows: int,
    max_analyze_columns: int,
) -> pd.DataFrame:
    delimiter = detect_csv_delimiter(file_bytes)

    attempts = []
    encodings = ["utf-8-sig", "utf-8", "utf-16", "latin-1"]
    for encoding in encodings:
        if delimiter:
            attempts.append({"engine": "python", "sep": delimiter, "encoding": encoding})
        attempts.append({"engine": "python", "sep": None, "encoding": encoding})
    attempts.append({})

    errors = []
    for csv_kwargs in attempts:
        try:
            header_df = pd.read_csv(io.BytesIO(file_bytes), nrows=0, on_bad_lines="skip", **csv_kwargs)
            if header_df.shape[1] == 0:
                errors.append("No columns detected in CSV header")
                continue
            if header_df.shape[1] > max_analyze_columns:
                raise HTTPException(
                    status_code=413,
                    detail=f"Dataset has {header_df.shape[1]} columns. Maximum allowed is {max_analyze_columns}.",
                )

            return pd.read_csv(
                io.BytesIO(file_bytes),
                nrows=max_analyze_rows + 1,
                on_bad_lines="skip",
                **csv_kwargs,
            )
        except HTTPException:
            raise
        except Exception as exc:
            errors.append(str(exc))

    first_error = errors[0] if errors else "Unknown CSV parse error"
    raise HTTPException(status_code=422, detail=f"Could not parse file: {first_error}")
