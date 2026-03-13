from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from collections import deque
import os
import re
import math
import json
import time
import requests
import xml.etree.ElementTree as ET
from pyproj import Transformer
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# KONFIGURACJA
# ---------------------------------------------------------------------------

transformer_2180 = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)

OPENLS_URL       = "http://mapy.geoportal.gov.pl/openLSgp/geocode"
WFS_RCN_URL      = "https://mapy.geoportal.gov.pl/wss/service/rcn"
WFS_RCN_URL_IP   = "https://91.223.135.44/wss/service/rcn"
GUGIK_HOST       = "mapy.geoportal.gov.pl"
ULDK_URL         = "https://uldk.gugik.gov.pl/"
WARSZAWA_WFS_URL = "https://wms2.um.warszawa.pl/geoserver/wfs/wfs"

MS_NS  = 'http://mapserver.gis.umn.edu/mapserver'
GML_NS = 'http://www.opengis.net/gml/3.2'

# ---------------------------------------------------------------------------
# ANALITYKA API (in-memory)
# ---------------------------------------------------------------------------

api_log = deque(maxlen=10000)
UPTIME_SINCE = datetime.utcnow().isoformat()


def log_api_call(address, status, response_ms, transactions_count=0, error_message=None):
    api_log.append({
        'ts': datetime.utcnow().isoformat(),
        'address': address,
        'status': status,
        'response_ms': response_ms,
        'transactions': transactions_count,
        'error': error_message,
    })


# ---------------------------------------------------------------------------
# GEOCODING
# ---------------------------------------------------------------------------

def normalize_address(raw: str) -> str:
    s = raw.strip()
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'\s*,\s*', ', ', s)
    return s


def smart_geocode(raw: str) -> dict | None:
    clean = normalize_address(raw)

    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": clean, "format": "json", "limit": 1,
                    "countrycodes": "pl", "addressdetails": 1},
            headers={"User-Agent": "PillarScout/8.0 (bartek.kulita@gmail.com)"},
            timeout=10,
        )
        data = r.json()
        if data:
            hit = data[0]
            addr = hit.get("address", {})
            city  = addr.get("city") or addr.get("town") or addr.get("village") or ""
            road  = addr.get("road", "")
            house = addr.get("house_number", "")
            canonical = ", ".join(filter(None, [city, f"{road} {house}".strip()]))
            return {
                "lon":       float(hit["lon"]),
                "lat":       float(hit["lat"]),
                "canonical": canonical or clean,
                "source":    "OSM Nominatim",
            }
    except Exception:
        pass

    try:
        xml_body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<xls:XLS xmlns:xls="http://www.opengis.net/xls" '
            'xmlns:gml="http://www.opengis.net/gml" version="1.2">'
            '<xls:RequestHeader/>'
            '<xls:Request methodName="GeocodeRequest" requestID="1" version="1.2">'
            '<xls:GeocodeRequest><xls:Address countryCode="PL">'
            f'<xls:freeFormAddress>{clean}</xls:freeFormAddress>'
            '</xls:Address></xls:GeocodeRequest></xls:Request></xls:XLS>'
        )
        r = requests.post(
            OPENLS_URL,
            data=xml_body.encode('utf-8'),
            headers={"Content-Type": "application/xml"},
            timeout=10,
        )
        root = ET.fromstring(r.content)
        pos = root.find('.//{http://www.opengis.net/gml}pos')
        if pos is not None and pos.text:
            lon, lat = map(float, pos.text.strip().split())
            return {"lon": lon, "lat": lat, "canonical": clean, "source": "OpenLS GUGiK"}
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# KONDYGNACJE - WFS Warszawa
# ---------------------------------------------------------------------------

def get_building_params(lon: float, lat: float) -> dict | None:
    d = 0.0003
    bbox = f"{lon-d},{lat-d},{lon+d},{lat+d},EPSG:4326"
    try:
        r = requests.get(
            WARSZAWA_WFS_URL,
            params={
                "service": "WFS", "version": "1.1.0", "request": "GetFeature",
                "typeName": "msw:budynki", "outputFormat": "application/json",
                "srsName": "EPSG:4326", "bbox": bbox,
            },
            timeout=8,
        )
        if r.status_code == 200:
            features = r.json().get('features', [])
            if features:
                kondygnacje = features[0].get('properties', {}).get('KONDYGNACJE_NADZIEMNE')
                if kondygnacje:
                    return {'kondygnacje': str(kondygnacje)}
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# DANE KATASTRALNE - ULDK GUGiK
# ---------------------------------------------------------------------------

def get_uldk_building(lon: float, lat: float) -> dict | None:
    easting, northing = transformer_2180.transform(lon, lat)

    raw_fields = None
    for req_type in ('GetBuildingByXY', 'GetParcelByXY'):
        try:
            r = requests.get(
                ULDK_URL,
                params={
                    'request': req_type,
                    'xy':      f'{easting:.2f},{northing:.2f}',
                    'result':  'id,voivodeship,county,commune,region,parcel',
                },
                timeout=8,
            )
            lines = r.text.strip().splitlines()
            if len(lines) >= 2 and lines[0].strip() == '0':
                fields = lines[1].replace(';', '|').split('|')
                if len(fields) >= 6:
                    raw_fields = fields
                    break
        except Exception:
            continue

    if not raw_fields:
        return None

    bid         = raw_fields[0].strip()
    wojewodztwo = raw_fields[1].strip()
    powiat      = raw_fields[2].strip()
    gmina       = raw_fields[3].strip()
    obreb       = raw_fields[4].strip()

    nr_dzialki = ''
    nr_budynku = ''
    try:
        segs = bid.split('_')
        if len(segs) >= 2:
            sub = segs[1].split('.')
            if len(sub) >= 3:
                nr_dzialki = sub[1]
                nr_budynku = sub[2]
            elif len(sub) == 2:
                nr_dzialki = sub[1]
    except Exception:
        pass

    powiat_clean = powiat.replace('powiat ', '').strip()

    dzielnica = ''
    try:
        r_rev = requests.get(
            'https://nominatim.openstreetmap.org/reverse',
            params={'lat': lat, 'lon': lon, 'format': 'json', 'addressdetails': 1},
            headers={'User-Agent': 'PillarScout/8.0 (bartek.kulita@gmail.com)'},
            timeout=5,
        )
        rev_addr = r_rev.json().get('address', {})
        dzielnica = rev_addr.get('suburb') or rev_addr.get('quarter') or ''
    except Exception:
        pass

    return {
        'id':          bid,
        'wojewodztwo': wojewodztwo,
        'powiat':      powiat_clean,
        'gmina':       gmina,
        'dzielnica':   dzielnica,
        'obreb':       obreb,
        'nr_dzialki':  nr_dzialki,
        'nr_budynku':  nr_budynku,
    }


# ---------------------------------------------------------------------------
# CENY TRANSAKCYJNE - RCN WFS API
# ---------------------------------------------------------------------------

def _get_centroid(feat):
    pos = feat.find(f'.//{{{GML_NS}}}pos')
    if pos is not None and pos.text:
        parts = pos.text.split()
        if len(parts) >= 2:
            return float(parts[0]), float(parts[1])
    lc = feat.find(f'.//{{{GML_NS}}}lowerCorner')
    uc = feat.find(f'.//{{{GML_NS}}}upperCorner')
    if lc is not None and uc is not None and lc.text and uc.text:
        l = list(map(float, lc.text.split()))
        u = list(map(float, uc.text.split()))
        if len(l) >= 2 and len(u) >= 2:
            return (l[0] + u[0]) / 2, (l[1] + u[1]) / 2
    return None, None


def _get_field(feat, *names):
    for name in names:
        el = feat.find(f'{{{MS_NS}}}{name}')
        if el is not None and el.text and el.text.strip() not in ['-', '']:
            return el.text.strip()
    return ''


def _query_rcn_layer(easting, northing, radius_m, layer, count, start):
    bbox = (f"{northing - radius_m},{easting - radius_m},"
            f"{northing + radius_m},{easting + radius_m},EPSG:2180")
    params = {
        'SERVICE':      'WFS',
        'VERSION':      '2.0.0',
        'REQUEST':      'GetFeature',
        'TYPENAMES':    layer,
        'COUNT':        str(count),
        'STARTINDEX':   str(start),
        'BBOX':         bbox,
        'outputFormat': 'application/gml+xml; version=3.2',
    }
    # Najpierw próba normalnego DNS
    try:
        r = requests.get(WFS_RCN_URL, params=params, timeout=25,
                         headers={'User-Agent': 'Mozilla/5.0'})
        return ET.fromstring(r.content.decode('utf-8', errors='replace'))
    except requests.exceptions.ConnectionError:
        # Fallback: bezpośredni IP z nagłówkiem Host (obejście DNS)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.get(WFS_RCN_URL_IP, params=params, timeout=25,
                         headers={'User-Agent': 'Mozilla/5.0', 'Host': GUGIK_HOST},
                         verify=False)
        return ET.fromstring(r.content.decode('utf-8', errors='replace'))


def _parse_adres(raw: str) -> str:
    if not raw:
        return ''
    if ';' in raw and ':' in raw:
        parts = dict(p.split(':', 1) for p in raw.split(';') if ':' in p)
        ul = parts.get('UL', '')
        nr = parts.get('NR_PORZ', '')
        ul = ul.replace('ulica ', 'ul. ').replace('aleja ', 'al. ').replace('Aleja ', 'al. ')
        return f'{ul} {nr}'.strip()
    return raw.strip()


def _format_kond(val: str) -> str:
    try:
        k = int(val)
        if k < 0:  return 'podziemie'
        if k == 0: return ''
        if k == 1: return 'parter'
        return f'p. {k - 1}'
    except (ValueError, TypeError):
        return ''


def get_rcn_prices(lon: float, lat: float, radius: int,
                   min_cena_m2: int = 3000,
                   typ: str = 'mieszkalna',
                   rynek: str = 'wtorny') -> tuple:
    easting, northing = transformer_2180.transform(lon, lat)

    def fetch_lokale():
        recs = []
        start = 0
        while True:
            try:
                root    = _query_rcn_layer(easting, northing, radius, 'ms:lokale', 200, start)
                members = [m for m in root if 'member' in m.tag.lower()]
                if not members:
                    break
                for member in members:
                    for feat in member:
                        n_c, e_c = _get_centroid(feat)
                        dist = (round(math.sqrt((northing - n_c) ** 2 + (easting - e_c) ** 2))
                                if n_c is not None else None)
                        recs.append({
                            'layer_type':    'lokale',
                            'dist':          dist,
                            'data':          _get_field(feat, 'dok_data')[:10],
                            'cena_brutto':   _get_field(feat, 'tran_cena_brutto'),
                            'pow':           _get_field(feat, 'lok_pow_uzyt'),
                            'rodzaj_rynku':  _get_field(feat, 'tran_rodzaj_rynku'),
                            'lok_funkcja':   _get_field(feat, 'lok_funkcja'),
                            'lok_izby':      _get_field(feat, 'lok_liczba_izb'),
                            'lok_kond':      _get_field(feat, 'lok_nr_kond'),
                            'lok_pow_przyn': _get_field(feat, 'lok_pow_przyn'),
                            'adres_raw':     _get_field(feat, 'lok_adres'),
                        })
                if len(members) < 200:
                    break
                start += 200
            except Exception:
                break
        return recs

    def fetch_dzialki():
        recs = []
        start = 0
        while True:
            try:
                root    = _query_rcn_layer(easting, northing, radius, 'ms:dzialki', 200, start)
                members = [m for m in root if 'member' in m.tag.lower()]
                if not members:
                    break
                for member in members:
                    for feat in member:
                        n_c, e_c = _get_centroid(feat)
                        dist = (round(math.sqrt((northing - n_c) ** 2 + (easting - e_c) ** 2))
                                if n_c is not None else None)
                        pow_ha = _get_field(feat, 'dzi_pow_ewid', 'nier_pow_gruntu')
                        pow_m2 = ''
                        try:
                            if pow_ha:
                                pow_m2 = str(round(float(pow_ha) * 10000, 1))
                        except (ValueError, TypeError):
                            pass
                        recs.append({
                            'layer_type':    'dzialki',
                            'dist':          dist,
                            'data':          _get_field(feat, 'dok_data')[:10],
                            'cena_brutto':   _get_field(feat, 'dzi_cena_brutto', 'nier_cena_brutto'),
                            'pow':           pow_m2,
                            'rodzaj_rynku':  _get_field(feat, 'tran_rodzaj_rynku'),
                            'lok_funkcja':   '',
                            'lok_izby':      '',
                            'lok_kond':      '',
                            'lok_pow_przyn': '',
                            'adres_raw':     _get_field(feat, 'dzi_nr_dzialki', 'dzi_id_dzialki'),
                            'rodzaj_uzytku': _get_field(feat, 'dzi_sposob_uzyt'),
                        })
                if len(members) < 200:
                    break
                start += 200
            except Exception:
                break
        return recs

    if typ in ('mieszkalna', 'handlowoUslugowa', 'biurowa', 'inne'):
        layers_to_fetch = [('lokale', fetch_lokale)]
    elif typ == 'dzialki':
        layers_to_fetch = [('dzialki', fetch_dzialki)]
    else:
        layers_to_fetch = [('lokale', fetch_lokale), ('dzialki', fetch_dzialki)]

    raw_records = []
    with ThreadPoolExecutor(max_workers=len(layers_to_fetch)) as executor:
        futures = {executor.submit(fn): name for name, fn in layers_to_fetch}
        for future in as_completed(futures):
            try:
                raw_records.extend(future.result())
            except Exception:
                pass

    transactions = []
    seen = set()

    for rec in raw_records:
        layer = rec['layer_type']

        rodzaj = rec.get('rodzaj_rynku', '')
        if rynek == 'wtorny'    and rodzaj == 'pierwotny': continue
        if rynek == 'pierwotny' and rodzaj == 'wtorny':    continue

        if layer == 'lokale' and typ not in ('wszystkie',):
            funkcja = rec.get('lok_funkcja', '')
            if typ == 'mieszkalna':
                if funkcja and funkcja != 'mieszkalna':
                    continue
            else:
                if funkcja != typ:
                    continue

        if not rec['cena_brutto'] or not rec['data']:
            continue
        try:
            cena = float(rec['cena_brutto'])
        except ValueError:
            continue
        if cena < 50000:
            continue

        key = (rec['data'], round(cena), layer)
        if key in seen:
            continue
        seen.add(key)

        metraz  = None
        cena_m2 = None
        try:
            if rec['pow'] and float(rec['pow']) > 1:
                metraz  = round(float(rec['pow']), 1)
                cena_m2 = round(cena / metraz)
        except (ValueError, ZeroDivisionError):
            pass

        if layer == 'lokale' and typ == 'mieszkalna':
            if metraz  and metraz  > 300:          continue
            if cena_m2 and cena_m2 < min_cena_m2: continue

        pow_przyn = None
        if layer == 'lokale':
            try:
                v = float(rec.get('lok_pow_przyn', '') or 0)
                if v > 0:
                    pow_przyn = round(v, 1)
            except (ValueError, TypeError):
                pass

        transactions.append({
            'layer_type':    layer,
            'data':          rec['data'],
            'cena':          round(cena),
            'cena_m2':       cena_m2,
            'metraz':        metraz,
            'ile':           1,
            'dist':          rec['dist'],
            'pokoje':        rec.get('lok_izby', '') if layer == 'lokale' else '',
            'pietro':        _format_kond(rec.get('lok_kond', '')) if layer == 'lokale' else '',
            'pow_przyn':     pow_przyn,
            'adres':         _parse_adres(rec.get('adres_raw', '')),
            'rodzaj_uzytku': rec.get('rodzaj_uzytku', '') if layer == 'dzialki' else '',
        })

    transactions.sort(key=lambda x: x['dist'] or 9999)
    transactions.sort(key=lambda x: x['data'] or '', reverse=True)
    transactions = transactions[:100]

    stats = None
    if transactions:
        cutoff = (date.today() - timedelta(days=180)).isoformat()
        recent = [t for t in transactions if t['data'] >= cutoff and t['cena_m2']]
        m2_prices = [t['cena_m2'] for t in recent]
        stats = {
            'avg':          round(sum(m2_prices) / len(m2_prices)) if m2_prices else None,
            'min':          round(min(m2_prices)) if m2_prices else None,
            'max':          round(max(m2_prices)) if m2_prices else None,
            'count':        len(transactions),
            'recent_count': len(recent),
        }

    return transactions, stats


# ---------------------------------------------------------------------------
# FLASK ROUTES
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET", "POST"])
def index():
    report = None
    radius = 200
    typ    = 'mieszkalna'
    rynek  = 'wtorny'

    if request.method == "POST":
        addr = request.form.get("address", "").strip()
        try:
            radius = int(request.form.get("radius", 200))
        except ValueError:
            radius = 200
        typ   = request.form.get("typ", "mieszkalna")
        rynek = request.form.get("rynek", "wtorny")

        t0 = time.time()
        try:
            geo = smart_geocode(addr)
            if geo:
                lon, lat    = geo["lon"], geo["lat"]
                is_warsaw   = 'warszawa' in (geo['canonical'] + addr).lower()
                min_cena_m2 = 5000 if is_warsaw else 3000

                trans, stats = get_rcn_prices(lon, lat, radius,
                                              min_cena_m2=min_cena_m2,
                                              typ=typ, rynek=rynek)

                elapsed_ms = round((time.time() - t0) * 1000)

                if not trans:
                    log_api_call(addr, 'empty', elapsed_ms, 0)
                else:
                    log_api_call(addr, 'ok', elapsed_ms, len(trans))

                cadastral = get_uldk_building(lon, lat)
                wfs_data  = get_building_params(lon, lat)
                building  = cadastral or {}
                if wfs_data and wfs_data.get('kondygnacje'):
                    building['kondygnacje'] = wfs_data['kondygnacje']
                building = building or None

                report = {
                    "address":        geo["canonical"],
                    "address_raw":    addr,
                    "geocode_source": geo["source"],
                    "radius":         radius,
                    "transactions":   trans,
                    "stats":          stats,
                    "building":       building,
                    "data_source":    "api",
                    "db_last_crawl":  "",
                    "typ":            typ,
                }
            else:
                elapsed_ms = round((time.time() - t0) * 1000)
                log_api_call(addr, 'geocode_fail', elapsed_ms, 0, 'Nie znaleziono adresu')
                report = {
                    "error": (
                        "Nie znaleziono adresu. "
                        'Podaj ulicę z numerem, np. "Płock, Dretkiewicza 21".'
                    ),
                    "address_raw": addr,
                }
        except requests.Timeout as e:
            elapsed_ms = round((time.time() - t0) * 1000)
            log_api_call(addr, 'timeout', elapsed_ms, 0, str(e))
            report = {
                "error": "API RCN nie odpowiedziało w wyznaczonym czasie. Spróbuj ponownie.",
                "address_raw": addr,
            }
        except Exception as e:
            elapsed_ms = round((time.time() - t0) * 1000)
            log_api_call(addr, 'error', elapsed_ms, 0, f'{type(e).__name__}: {e}')
            import traceback
            tb = traceback.format_exc()
            app.logger.error("Błąd przy przetwarzaniu adresu '%s':\n%s", addr, tb)
            report = {
                "error": f"Błąd wewnętrzny: {type(e).__name__}: {e}",
                "address_raw": addr,
            }

    return render_template("index.html", report=report, radius=radius, typ=typ, rynek=rynek)


# ---------------------------------------------------------------------------
# API ENDPOINT - JSON
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    addr   = request.args.get("address", "").strip()
    radius = int(request.args.get("radius", 200))
    typ    = request.args.get("typ", "mieszkalna")
    rynek  = request.args.get("rynek", "wtorny")

    if not addr:
        return jsonify({"error": "Brak parametru address"}), 400

    t0 = time.time()
    try:
        geo = smart_geocode(addr)
        if not geo:
            elapsed_ms = round((time.time() - t0) * 1000)
            log_api_call(addr, 'geocode_fail', elapsed_ms, 0, 'Nie znaleziono adresu')
            return jsonify({"error": "Nie znaleziono adresu"}), 404

        lon, lat    = geo["lon"], geo["lat"]
        is_warsaw   = 'warszawa' in (geo['canonical'] + addr).lower()
        min_cena_m2 = 5000 if is_warsaw else 3000

        trans, stats = get_rcn_prices(lon, lat, radius,
                                      min_cena_m2=min_cena_m2,
                                      typ=typ, rynek=rynek)

        elapsed_ms = round((time.time() - t0) * 1000)

        if not trans:
            log_api_call(addr, 'empty', elapsed_ms, 0)
        else:
            log_api_call(addr, 'ok', elapsed_ms, len(trans))

        return jsonify({
            "address":        geo["canonical"],
            "geocode_source": geo["source"],
            "radius":         radius,
            "typ":            typ,
            "rynek":          rynek,
            "transactions":   trans,
            "stats":          stats,
            "response_ms":    elapsed_ms,
        })

    except requests.Timeout as e:
        elapsed_ms = round((time.time() - t0) * 1000)
        log_api_call(addr, 'timeout', elapsed_ms, 0, str(e))
        return jsonify({"error": "API RCN timeout"}), 504

    except Exception as e:
        elapsed_ms = round((time.time() - t0) * 1000)
        log_api_call(addr, 'error', elapsed_ms, 0, f'{type(e).__name__}: {e}')
        return jsonify({"error": f"Błąd: {type(e).__name__}: {e}"}), 500


# ---------------------------------------------------------------------------
# API DEBUG - diagnostyka RCN
# ---------------------------------------------------------------------------

@app.route("/api/debug")
def api_debug():
    addr   = request.args.get("address", "Warszawa, Marysienki 19").strip()
    radius = int(request.args.get("radius", 300))

    geo = smart_geocode(addr)
    if not geo:
        return jsonify({"error": "Geocode failed"}), 404

    lon, lat = geo["lon"], geo["lat"]
    easting, northing = transformer_2180.transform(lon, lat)

    bbox = (f"{northing - radius},{easting - radius},"
            f"{northing + radius},{easting + radius},EPSG:2180")

    debug_info = {
        "address": geo["canonical"],
        "lon": lon, "lat": lat,
        "easting": round(easting, 1),
        "northing": round(northing, 1),
        "bbox": bbox,
        "radius": radius,
    }

    # Raw request do API RCN (z fallback na IP)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for layer in ['ms:lokale', 'ms:dzialki']:
        params = {
            'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature',
            'TYPENAMES': layer, 'COUNT': '10', 'STARTINDEX': '0',
            'BBOX': bbox,
            'outputFormat': 'application/gml+xml; version=3.2',
        }
        try:
            try:
                r = requests.get(WFS_RCN_URL, params=params, timeout=25,
                                 headers={'User-Agent': 'Mozilla/5.0'})
            except requests.exceptions.ConnectionError:
                r = requests.get(WFS_RCN_URL_IP, params=params, timeout=25,
                                 headers={'User-Agent': 'Mozilla/5.0', 'Host': GUGIK_HOST},
                                 verify=False)
            raw_text = r.content.decode('utf-8', errors='replace')[:3000]
            root = ET.fromstring(r.content.decode('utf-8', errors='replace'))
            members = [m for m in root if 'member' in m.tag.lower()]
            debug_info[layer] = {
                "status_code": r.status_code,
                "members_count": len(members),
                "url": r.url[:500],
                "raw_first_2000": raw_text[:2000],
            }
        except Exception as e:
            debug_info[layer] = {"error": str(e)}

    return jsonify(debug_info)


# ---------------------------------------------------------------------------
# API STATS - ANALITYKA
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    now = datetime.utcnow()
    cutoff_24h = (now - timedelta(hours=24)).isoformat()

    all_entries = list(api_log)
    last_24h = [e for e in all_entries if e['ts'] >= cutoff_24h]

    def calc_stats(entries):
        total = len(entries)
        ok = sum(1 for e in entries if e['status'] == 'ok')
        error = sum(1 for e in entries if e['status'] == 'error')
        timeout = sum(1 for e in entries if e['status'] == 'timeout')
        empty = sum(1 for e in entries if e['status'] == 'empty')
        geocode_fail = sum(1 for e in entries if e['status'] == 'geocode_fail')
        times = [e['response_ms'] for e in entries if e['response_ms'] is not None]
        avg_ms = round(sum(times) / len(times)) if times else 0
        failure_rate = round((error + timeout) / total * 100, 1) if total > 0 else 0
        return {
            'total': total,
            'ok': ok,
            'error': error,
            'timeout': timeout,
            'empty': empty,
            'geocode_fail': geocode_fail,
            'failure_rate': failure_rate,
            'avg_response_ms': avg_ms,
        }

    errors_only = [e for e in all_entries if e['status'] in ('error', 'timeout')]
    last_errors = errors_only[-10:] if errors_only else []

    result = calc_stats(all_entries)
    result['last_24h'] = calc_stats(last_24h)
    result['uptime_since'] = UPTIME_SINCE
    result['last_errors'] = last_errors

    return jsonify(result)


# ---------------------------------------------------------------------------
# HEALTH CHECK
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "8.0-railway"})


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8282))
    app.run(debug=False, port=port, host="0.0.0.0")
