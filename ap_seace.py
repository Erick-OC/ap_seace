import os
import json
import hashlib
from datetime import datetime, timezone
import time

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OECE_RECORDS_URL = os.getenv("OECE_RECORDS_URL")
MAX_PAGES = 5

def validar_config() -> None:
    faltantes = []

    if not SUPABASE_URL:
        faltantes.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        faltantes.append("SUPABASE_KEY")
    if not OECE_RECORDS_URL:
        faltantes.append("OECE_RECORDS_URL")

    if faltantes:
        raise ValueError(f"Faltan variables en .env: {', '.join(faltantes)}")


validar_config()

supabase = create_client(
    SUPABASE_URL.strip(),
    SUPABASE_KEY.strip()
)


def generar_hash(data: dict) -> str:
    contenido = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


def mapear_record(record: dict) -> dict:
    compiled = record.get("compiledRelease", {})
    tender = compiled.get("tender", {})
    buyer = compiled.get("buyer", {})
    value = tender.get("value", {})

    sources = compiled.get("sources", [])
    source_url = sources[0].get("url") if sources else None

    return {
        "ocid": compiled.get("ocid"),
        "release_id": compiled.get("id"),
        "tender_id": tender.get("id"),
        "nomenclatura": tender.get("title"),
        "buyer_name": buyer.get("name"),
        "entity_name": tender.get("procuringEntity", {}).get("name"),
        "title": tender.get("title"),
        "description": tender.get("description"),
        "procurement_method": tender.get("procurementMethod"),
        "procurement_method_details": tender.get("procurementMethodDetails"),
        "main_procurement_category": tender.get("mainProcurementCategory"),
        "amount": value.get("amount"),
        "currency": value.get("currency"),
        "tender_status": tender.get("status"),
        "published_date": compiled.get("publishedDate"),
        "date_published": tender.get("datePublished"),
        "enquiry_end_date": tender.get("enquiryPeriod", {}).get("endDate"),
        "submission_end_date": tender.get("tenderPeriod", {}).get("endDate"),
        "source_url": source_url,
        "compiled_release_json": compiled,
        "content_hash": generar_hash(compiled),
        "last_seen_at": datetime.now(timezone.utc).isoformat()
    }


def upsert_si_cambio(data: dict) -> str:
    try:
        existente = (
            supabase.table("licitaciones")
            .select("ocid, content_hash")
            .eq("ocid", data["ocid"])
            .limit(1)
            .execute()
        )

        if existente.data:
            actual = existente.data[0]
            if actual.get("content_hash") == data["content_hash"]:
                return "sin_cambios"

        supabase.table("licitaciones").upsert(
            data,
            on_conflict="ocid"
        ).execute()

        return "actualizado"

    except Exception as e:
        print(f"Error en OCID {data.get('ocid')}: {e}")
        return "error"


def main() -> None:
    url = OECE_RECORDS_URL
    procesados = 0
    actualizados = 0
    sin_cambios = 0
    paginas = 0

    while url and paginas < MAX_PAGES:
        paginas += 1
        print(f"[Página {paginas}] Fetching: {url}")

        res = requests.get(url, timeout=30)

        if res.status_code == 404:
            print(f"Fin de paginación en: {url}")
            break

        res.raise_for_status()

        
        data = res.json()
        records = data.get("records", [])

        for record in records:
            fila = mapear_record(record)

            if not fila["ocid"]:
                continue

            estado = upsert_si_cambio(fila)
            procesados += 1

            time.sleep(0.1) 

            if estado == "actualizado":
                actualizados += 1
            else:
                sin_cambios += 1

        url = data.get("links", {}).get("next")
        time.sleep(1)

    print("Sync completo")
    print(f"  Registros procesados: {procesados}")
    print(f"  Insertados/actualizados: {actualizados}")
    print(f"  Sin cambios: {sin_cambios}")
    print(f"  Páginas recorridas: {paginas}")


if __name__ == "__main__":
    main()