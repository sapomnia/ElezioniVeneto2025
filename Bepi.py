#!/usr/bin/env python3
"""
Scraper Eligendo - Regionali Veneto 2025
Estrae per ogni sezione:
- Voti alla lista "LEGA - LIGA VENETA STEFANI PRESIDENTE"
- Preferenze per "ZAIA LUCA"

API Endpoints (da HAR catturato):
- Anagrafica: https://eleapi.interno.gov.it/siel/PX/getentiRZ/DE/20251123/TE/07/RE/05
- Scrutini:   https://eleapi.interno.gov.it/siel/PX/scrutiniR/DE/20251123/TE/07/RE/05/PR/{prov}/CM/{com}/SZ/{sez}
- Preferenze: https://eleapi.interno.gov.it/siel/PX/getprefeR/DE/20251123/TE/07/RE/05/PR/{prov}/CM/{com}/SZ/{sez}
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import csv
import time
import os
from datetime import datetime

# Configurazione
BASE_URL = "https://eleapi.interno.gov.it/siel/PX"
DATA_ELEZIONE = "20251123"
TIPO_ELEZIONE = "07"  # Regionali
REGIONE = "05"  # Veneto

# Headers per le richieste
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://elezioni.interno.gov.it/'
}

# Cartella output
OUTPUT_DIR = "output_veneto_2025"

# Configura sessione con retry automatici
def create_session():
    """Crea sessione HTTP con retry automatici per errori di rete"""
    session = requests.Session()
    
    # Retry su errori di connessione, timeout, e alcuni status code
    retry_strategy = Retry(
        total=5,  # Max 5 tentativi
        backoff_factor=1,  # Attesa: 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

# Sessione globale
SESSION = None

def get_session():
    global SESSION
    if SESSION is None:
        SESSION = create_session()
    return SESSION


def get_anagrafica():
    """Scarica l'anagrafica completa di tutti gli enti del Veneto"""
    url = f"{BASE_URL}/getentiRZ/DE/{DATA_ELEZIONE}/TE/{TIPO_ELEZIONE}/RE/{REGIONE}"
    print(f"Scaricamento anagrafica da: {url}")
    
    session = get_session()
    response = session.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    
    # Salva JSON per debug
    with open(f"{OUTPUT_DIR}/anagrafica_veneto.json", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return data


def parse_codice_13(codice):
    """
    Parsa il codice ente a 13 cifre di getentiRZ.
    
    Struttura verificata (es: 0508704200001 = Venezia sez 1):
    - [0:2]  = regione (05 = Veneto)
    - [2:5]  = provincia (087 = Venezia) → PR/087
    - [5:9]  = comune (0420 = Venezia)   → CM/0420
    - [9:13] = sezione (0001 = sez 1)    → SZ/0001
    
    Per comuni: sezione = 0000
    Per sezioni: sezione > 0
    """
    if len(codice) != 13:
        return None
    
    return {
        'regione': codice[0:2],
        'provincia': codice[2:5],   # Codice API provincia
        'comune': codice[5:9],      # Codice API comune
        'sezione': codice[9:13],    # Codice API sezione
        'raw': codice
    }


def build_mapping_from_anagrafica(enti):
    """
    Costruisce il mapping provincia/comune/sezioni dall'anagrafica.
    
    Struttura codici a 13 cifre (verificata con HAR):
    - 05|087|0420|0001 = Venezia sezione 1
    - URL: PR/087/CM/0420/SZ/0001
    """
    province = {}  # {nome_prov: {cod_api: xxx, comuni: {nome_com: {cod_api: xxx, sezioni: [...]}}}}
    
    current_prov = None
    current_com = None
    
    for ente in enti:
        tipo = ente.get('tipo')
        desc = ente.get('desc', '')
        cod = ente.get('cod', '')
        
        parsed = parse_codice_13(cod)
        if not parsed:
            continue
        
        if tipo == 'RE':
            # Regione - skip
            continue
        elif tipo == 'PR':
            # Provincia
            current_prov = desc
            province[current_prov] = {
                'cod_api': parsed['provincia'],  # Es: 087
                'cod_raw': cod,
                'comuni': {}
            }
        elif tipo == 'CM':
            # Comune
            current_com = desc
            if current_prov and current_prov in province:
                province[current_prov]['comuni'][current_com] = {
                    'cod_api': parsed['comune'],  # Es: 0420
                    'cod_prov': parsed['provincia'],  # Serve per costruire URL
                    'cod_raw': cod,
                    'sezioni': []
                }
        elif tipo == 'SZ':
            # Sezione
            if current_prov and current_com:
                if current_prov in province and current_com in province[current_prov]['comuni']:
                    province[current_prov]['comuni'][current_com]['sezioni'].append({
                        'num': parsed['sezione'],  # Es: 0001
                        'cod_raw': cod
                    })
    
    return province


def get_scrutini_sezione(cod_prov, cod_com, cod_sez, max_retries=3):
    """Scarica i dati di scrutinio per una sezione con retry"""
    url = f"{BASE_URL}/scrutiniR/DE/{DATA_ELEZIONE}/TE/{TIPO_ELEZIONE}/RE/{REGIONE}/PR/{cod_prov}/CM/{cod_com}/SZ/{cod_sez}"
    
    session = get_session()
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2s, 4s, 6s
                print(f"\n    Errore connessione, retry {attempt+1}/{max_retries} tra {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  Errore scrutini {cod_prov}/{cod_com}/{cod_sez}: {e}")
                return None
        except Exception as e:
            print(f"  Errore scrutini {cod_prov}/{cod_com}/{cod_sez}: {e}")
            return None
    
    return None


def get_preferenze_sezione(cod_prov, cod_com, cod_sez, max_retries=3):
    """Scarica le preferenze per una sezione con retry"""
    url = f"{BASE_URL}/getprefeR/DE/{DATA_ELEZIONE}/TE/{TIPO_ELEZIONE}/RE/{REGIONE}/PR/{cod_prov}/CM/{cod_com}/SZ/{cod_sez}"
    
    session = get_session()
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2s, 4s, 6s
                print(f"\n    Errore connessione, retry {attempt+1}/{max_retries} tra {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  Errore preferenze {cod_prov}/{cod_com}/{cod_sez}: {e}")
                return None
        except Exception as e:
            print(f"  Errore preferenze {cod_prov}/{cod_com}/{cod_sez}: {e}")
            return None
    
    return None


def extract_voti_lega(scrutini_data):
    """
    Estrae i voti per la lista LEGA dallo scrutinio.
    
    Struttura JSON:
    - cand[]: candidati presidente
    - cand[x].liste[]: liste collegate al candidato
    - La LEGA è collegata a STEFANI
    """
    if not scrutini_data:
        return None
    
    candidati = scrutini_data.get('cand', [])
    
    for cand in candidati:
        # Cerca candidato Stefani
        if cand.get('cogn', '').upper() == 'STEFANI':
            liste = cand.get('liste', [])
            for lista in liste:
                desc = lista.get('desc_lis_c', '')
                if 'LEGA' in desc.upper():
                    return lista.get('voti', 0)
    
    return None


def extract_preferenze_zaia(preferenze_data):
    """
    Estrae le preferenze per ZAIA LUCA.
    
    Struttura JSON:
    - liste[]: elenco liste con codice
    - cand[]: candidati con cod_lis, cogn, nome, voti
    - ZAIA è nella lista LEGA (cod_lis = 16 per Venezia, ma può variare)
    """
    if not preferenze_data:
        return None
    
    candidati = preferenze_data.get('cand', [])
    
    for cand in candidati:
        cogn = cand.get('cogn', '').upper()
        nome = cand.get('nome', '').upper()
        
        if cogn == 'ZAIA' and nome == 'LUCA':
            return cand.get('voti', 0)
    
    return None


def load_processed_sections(csv_file):
    """Carica le sezioni già processate dal CSV esistente"""
    processed = set()
    
    if os.path.exists(csv_file):
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Chiave unica: cod_provincia/cod_comune/cod_sezione
                    key = f"{row['cod_provincia']}/{row['cod_comune']}/{row['cod_sezione']}"
                    processed.add(key)
            print(f"  Trovate {len(processed)} sezioni già processate")
        except Exception as e:
            print(f"  Errore lettura CSV esistente: {e}")
    
    return processed


def main(resume=True):
    """
    Funzione principale
    
    Args:
        resume: Se True, riprende da dove si era interrotto
    """
    
    # Crea cartella output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("=" * 60)
    print("SCRAPER ELIGENDO - REGIONALI VENETO 2025")
    print("Lista: LEGA - LIGA VENETA STEFANI PRESIDENTE")
    print("Candidato: ZAIA LUCA")
    print("=" * 60)
    print()
    
    # File CSV output
    csv_file = f"{OUTPUT_DIR}/risultati_veneto_2025.csv"
    
    # Carica sezioni già processate (se resume=True)
    processed_sections = set()
    file_mode = 'w'
    write_header = True
    
    if resume and os.path.exists(csv_file):
        print("Modalità RIPRESA attivata")
        processed_sections = load_processed_sections(csv_file)
        if processed_sections:
            file_mode = 'a'  # Append mode
            write_header = False
    
    # Step 1: Scarica anagrafica
    print("\nSTEP 1: Scaricamento anagrafica...")
    try:
        anagrafica = get_anagrafica()
        enti = anagrafica.get('enti', [])
        print(f"  Trovati {len(enti)} enti")
    except Exception as e:
        print(f"ERRORE: Impossibile scaricare anagrafica: {e}")
        print("\nProva a:")
        print("1. Verificare la connessione internet")
        print("2. Verificare che l'URL sia corretto")
        print("3. Provare con curl: curl -v '{url}'")
        return
    
    # Step 2: Costruisci mapping
    print("\nSTEP 2: Costruzione mapping province/comuni/sezioni...")
    province = build_mapping_from_anagrafica(enti)
    
    tot_comuni = sum(len(p['comuni']) for p in province.values())
    tot_sezioni = sum(
        len(c['sezioni']) 
        for p in province.values() 
        for c in p['comuni'].values()
    )
    
    sezioni_da_fare = tot_sezioni - len(processed_sections)
    
    print(f"  Province: {len(province)}")
    print(f"  Comuni: {tot_comuni}")
    print(f"  Sezioni totali: {tot_sezioni}")
    if processed_sections:
        print(f"  Sezioni già fatte: {len(processed_sections)}")
        print(f"  Sezioni da fare: {sezioni_da_fare}")
    
    # Salva mapping per debug
    with open(f"{OUTPUT_DIR}/mapping_province.json", 'w', encoding='utf-8') as f:
        json.dump(province, f, ensure_ascii=False, indent=2)
    
    # Step 3: Scarica dati per ogni sezione
    print(f"\nSTEP 3: Scaricamento dati sezioni...")
    print("  (questo potrebbe richiedere diversi minuti)")
    
    start_time = datetime.now()
    processed = 0
    skipped = 0
    errors_list = []
    
    with open(csv_file, file_mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                'provincia', 'cod_provincia',
                'comune', 'cod_comune', 
                'sezione', 'cod_sezione',
                'voti_lega', 'preferenze_zaia'
            ])
        
        for nome_prov, prov_data in province.items():
            cod_prov = prov_data['cod_api']
            print(f"\n  Provincia: {nome_prov} (cod: {cod_prov})")
            
            for nome_com, com_data in prov_data['comuni'].items():
                cod_com = com_data['cod_api']
                # Usa cod_prov dal comune per sicurezza (in caso di province "ereditate")
                cod_prov_actual = com_data.get('cod_prov', cod_prov)
                sezioni = com_data['sezioni']
                
                if not sezioni:
                    continue
                
                # Conta quante sezioni da fare per questo comune
                sezioni_da_fare_com = sum(
                    1 for sez in sezioni 
                    if f"{cod_prov_actual}/{cod_com}/{sez['num']}" not in processed_sections
                )
                
                if sezioni_da_fare_com == 0:
                    skipped += len(sezioni)
                    continue
                
                print(f"    Comune: {nome_com} ({sezioni_da_fare_com}/{len(sezioni)} sezioni)...", end='', flush=True)
                
                com_voti_lega = 0
                com_pref_zaia = 0
                com_errori = 0
                
                for sez in sezioni:
                    cod_sez = sez['num']
                    
                    # Salta se già processata
                    section_key = f"{cod_prov_actual}/{cod_com}/{cod_sez}"
                    if section_key in processed_sections:
                        continue
                    
                    # Scarica scrutini
                    scrutini = get_scrutini_sezione(cod_prov_actual, cod_com, cod_sez)
                    voti_lega = extract_voti_lega(scrutini)
                    
                    # Scarica preferenze
                    preferenze = get_preferenze_sezione(cod_prov_actual, cod_com, cod_sez)
                    pref_zaia = extract_preferenze_zaia(preferenze)
                    
                    # Scrivi CSV
                    writer.writerow([
                        nome_prov, cod_prov_actual,
                        nome_com, cod_com,
                        sez.get('num', '').lstrip('0') or '1', cod_sez,
                        voti_lega if voti_lega is not None else '',
                        pref_zaia if pref_zaia is not None else ''
                    ])
                    
                    # Flush per salvare subito (importante per ripresa!)
                    f.flush()
                    
                    if voti_lega is not None:
                        com_voti_lega += voti_lega
                    if pref_zaia is not None:
                        com_pref_zaia += pref_zaia
                    if voti_lega is None or pref_zaia is None:
                        com_errori += 1
                        errors_list.append(section_key)
                    
                    processed += 1
                    
                    # Rate limiting
                    time.sleep(0.1)
                
                status = f" LEGA:{com_voti_lega} ZAIA:{com_pref_zaia}"
                if com_errori > 0:
                    status += f" (err:{com_errori})"
                print(status)
                
                # Progress ogni 100 sezioni
                if processed % 100 == 0 and processed > 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = (sezioni_da_fare - processed) / rate if rate > 0 else 0
                    print(f"      Progress: {processed}/{sezioni_da_fare} ({rate:.1f}/s, ~{remaining/60:.0f}min)")
    
    # Riepilogo finale
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 60)
    print("COMPLETATO!")
    print(f"  Sezioni processate: {processed}")
    print(f"  Sezioni saltate (già fatte): {skipped}")
    print(f"  Errori: {len(errors_list)}")
    print(f"  Tempo totale: {elapsed/60:.1f} minuti")
    print(f"  File output: {csv_file}")
    
    if errors_list:
        print(f"\n  Sezioni con errori salvate in: {OUTPUT_DIR}/errori.txt")
        with open(f"{OUTPUT_DIR}/errori.txt", 'w') as f:
            f.write('\n'.join(errors_list))
    
    print("=" * 60)


def test_singola_sezione():
    """Test con una singola sezione (Venezia sez 1) per verificare che funzioni"""
    
    print("TEST: Venezia sezione 1")
    print("-" * 40)
    
    # Codici da HAR
    cod_prov = "087"  # Venezia
    cod_com = "0420"  # Venezia comune
    cod_sez = "0001"  # Sezione 1
    
    # Inizializza sessione
    global SESSION
    SESSION = create_session()
    
    # Test scrutini
    print(f"URL scrutini: {BASE_URL}/scrutiniR/DE/{DATA_ELEZIONE}/TE/{TIPO_ELEZIONE}/RE/{REGIONE}/PR/{cod_prov}/CM/{cod_com}/SZ/{cod_sez}")
    scrutini = get_scrutini_sezione(cod_prov, cod_com, cod_sez)
    
    if scrutini:
        print("  Scrutini OK")
        voti_lega = extract_voti_lega(scrutini)
        print(f"  Voti LEGA: {voti_lega}")
    else:
        print("  ERRORE scrutini")
    
    # Test preferenze
    print(f"URL preferenze: {BASE_URL}/getprefeR/DE/{DATA_ELEZIONE}/TE/{TIPO_ELEZIONE}/RE/{REGIONE}/PR/{cod_prov}/CM/{cod_com}/SZ/{cod_sez}")
    preferenze = get_preferenze_sezione(cod_prov, cod_com, cod_sez)
    
    if preferenze:
        print("  Preferenze OK")
        pref_zaia = extract_preferenze_zaia(preferenze)
        print(f"  Preferenze ZAIA: {pref_zaia}")
    else:
        print("  ERRORE preferenze")
    
    print()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg == "test":
            # Modalità test
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            test_singola_sezione()
        elif arg == "fresh":
            # Ricomincia da zero (ignora CSV esistente)
            main(resume=False)
        elif arg == "resume":
            # Riprendi esplicitamente
            main(resume=True)
        else:
            print("Uso:")
            print("  python eligendo_veneto_2025.py          # Scraping (riprende se interrotto)")
            print("  python eligendo_veneto_2025.py test     # Test singola sezione")
            print("  python eligendo_veneto_2025.py fresh    # Ricomincia da zero")
            print("  python eligendo_veneto_2025.py resume   # Riprendi da interruzione")
    else:
        # Default: scraping con resume automatico
        main(resume=True)