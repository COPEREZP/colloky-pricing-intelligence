# =========================================================
# COLLOKY PRICING-INTELLIGENCE SCRAPER - v3
# =========================================================
"""
Collects product data from Colloky's online store (colloky.cl) and saves a
time-stamped snapshot into PostgreSQL (table: colloky_prueba_f).

Every run is a new snapshot, so prices, discounts, stock and assortment can
be tracked over time. Historical rows are never updated or deleted.

What's new compared with v2
---------------------------
 1. New table name: `colloky_prueba_f` (the v2 table is not touched).
    The optional DDL file is now `colloky_schema.sql`.
 2. Columns `sizes_available` and `sizes_out_of_stock` removed
    (they were 100% NULL: VTEX does not expose size labels in the listing).
 3. Product description: for every product the scraper calls VTEX's public
    catalog API (/api/catalog_system/pub/products/search/{slug}/p) from
    inside the browser, in one batch per page. If that fails, it falls back
    to the `description` field of the listing JSON-LD (when present).
    The clean description is saved in the new column `product_description`.
 4. Material is detected with this PRIORITY:
      1st - the product "Composición" specification (e.g. "100% Algodón."),
            the same value the product page shows under "Especificaciones".
            It is the most precise source: when it yields a material,
            it is the ONLY source used.
      2nd - only if the composition is missing or has no known material:
            the product name + the product description, combined.
    In the description, ambiguous words are ignored to avoid false positives:
      "jean/jeans" (e.g. "combina con tus jeans") and "tejido/tejida"
      (in marketing text "tejido" usually just means "fabric").
    New materials typical of compositions: Polyester, Elastane, Nylon,
    Acrylic, Rubber, PVC, TPR, Synthetic (the last four only from the
    composition, because in names/descriptions they are ambiguous).
    New columns:
      - `material_source`: "specification", "name", "description",
        "name+description" or "Not Specified".
      - `material_composition`: the raw composition text, as published.
 5. Upsert with ON CONFLICT (product_url, date_scraped), same as v2.
 6. The description is split into the sections the product page shows,
    detected by their heading text (not by CSS ids, which change):
      - "Descripción"           -> description_main
      - "Consejos de cuidado"   -> care_tips
      - "Cuándo usarlo"         -> when_to_use
      - "Prendas que combinan"  -> pairs_with
    Text before the first heading goes to description_main. If the
    description has no headings, everything goes to description_main.
    VTEX specifications with those names are used as a fallback.
    Material detection from the description only uses description_main and
    care_tips ("Prendas que combinan" talks about OTHER products, e.g.
    "combínalo con una polera de algodón", which would be a false positive).
 7. Unattended runs:
      - Chrome runs without a visible window (EJECUTAR_SIN_VENTANA).
      - The computer is asked not to go to sleep while the scraper runs
        (EVITAR_SUSPENSION): Windows SetThreadExecutionState, macOS
        caffeinate, Linux systemd-inhibit. Released automatically at the end.
      - Everything printed is also written to logs/colloky_scraper_<date>.log
        (UTF-8), so a run in the background can be followed later.
 8. Resume an interrupted snapshot (REANUDAR_SNAPSHOT): pages already saved
    for that snapshot are skipped and the run continues where it stopped.
 9. Duplicate SKUs in the same run: the occurrence with LESS stock is kept
    (fewer sizes in stock; tie -> higher stockout_rate; full tie -> the
    first one). If the kept occurrence has another URL, the previous row of
    THIS snapshot is replaced. Rows of previous snapshots are never touched.
"""

import html
import json
import os
import platform
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from difflib import SequenceMatcher
from urllib.parse import urljoin, urlparse, urlunparse


# =========================================================
# SCRAPER CONFIGURATION
# =========================================================

MARCA = "Colloky"

# (category name saved in the table, category URL)
CATEGORIAS = [
    ("Accessories", "https://www.colloky.cl/accesorios?page={}"),
    ("Clothing", "https://www.colloky.cl/ropa?page={}"),
    ("Footwear", "https://www.colloky.cl/calzado?page={}"),
]

URL_TIENDA = "https://www.colloky.cl"

NOMBRE_TABLA = "colloky_prueba_f"

PAGINA_INICIAL = 1
PAGINA_FINAL = 99

# Prints the classification test table before scraping.
EJECUTAR_TESTS = True

# Fetch the product description from VTEX's catalog API (one batch per page).
# Set to False to scrape faster using only the product name (as in v2).
OBTENER_DESCRIPCIONES = True

# Timeout for each description request, in milliseconds.
TIMEOUT_DESCRIPCION_MS = 15000

# File with the CREATE TABLE / ADD COLUMN statements.
# If the file is not found, the same DDL embedded below is used.
ARCHIVO_SQL = "colloky_schema.sql"


# =========================================================
# UNATTENDED / BACKGROUND RUN
# =========================================================

# Chrome without a visible window. If the store ever stops returning
# products (empty pages from the first page), set it to False.
EJECUTAR_SIN_VENTANA = True

# Ask the operating system not to suspend while the scraper runs.
# (It cannot stop a MANUAL suspend or closing a laptop lid if the power
# settings say "sleep when the lid is closed".)
EVITAR_SUSPENSION = True

# Folder where each run writes its log (same text you see on screen).
CARPETA_LOGS = "logs"

# Resume an interrupted snapshot.
#   None -> new snapshot (normal weekly run).
#   "2026-10-06 08:47:00" -> continue THAT snapshot: pages already saved
#   are skipped. The exact date is printed at the start of every run and
#   is also in the log file name.
# It can also be set with the environment variable COLLOKY_REANUDAR_SNAPSHOT.
REANUDAR_SNAPSHOT = os.getenv("COLLOKY_REANUDAR_SNAPSHOT") or None


# =========================================================
# POSTGRESQL CONFIGURATION
# =========================================================

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")

engine = None
driver = None
wait = None

# Single timestamp for the whole run (rounded to the minute).
# When resuming, the timestamp of the interrupted snapshot is reused.
if REANUDAR_SNAPSHOT:
    DATE_SCRAPED = datetime.fromisoformat(str(REANUDAR_SNAPSHOT).strip())
else:
    DATE_SCRAPED = datetime.now().replace(second=0, microsecond=0)


def crear_engine():

    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    url_postgresql = URL.create(
        drivername="postgresql+psycopg2",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
    )

    return create_engine(url_postgresql, pool_pre_ping=True)


# =========================================================
# TABLE DEFINITION
# =========================================================
# Same content as colloky_schema.sql.
# Every statement is idempotent, so it is safe to run on every execution.

DDL_TABLA = f"""
CREATE TABLE IF NOT EXISTS {NOMBRE_TABLA} (
    id                  BIGSERIAL PRIMARY KEY,
    date_scraped        TIMESTAMP     NOT NULL,
    brand               VARCHAR(100)  NOT NULL,
    product_url         TEXT          NOT NULL
);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS category            VARCHAR(50);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS page_number         INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS page_position       INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS category_rank       INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS sku                 VARCHAR(50);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS sku_number          INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS image_url           TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS product_name        VARCHAR(500);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS product_name_raw    VARCHAR(500);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS product_description TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS description_main    TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS care_tips           TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS when_to_use         TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS pairs_with          TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS product_type        VARCHAR(100);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS gender              VARCHAR(30);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS age_segment         VARCHAR(30);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS age_min_months      INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS size_range          VARCHAR(100);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS is_licensed         BOOLEAN DEFAULT FALSE;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS license_brand       VARCHAR(50);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS license_character   VARCHAR(100);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS material            VARCHAR(200);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS material_source     VARCHAR(50);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS material_composition TEXT;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS color               VARCHAR(250);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS color_type          VARCHAR(50);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS color_count         INTEGER DEFAULT 0;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS original_price      INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS final_price         INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS discount            NUMERIC(6,4);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS offers_total        INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS offers_in_stock     INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS offers_out_of_stock INTEGER;
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS stockout_rate       NUMERIC(6,4);
ALTER TABLE {NOMBRE_TABLA} ADD COLUMN IF NOT EXISTS created_at          TIMESTAMP DEFAULT NOW();
CREATE UNIQUE INDEX IF NOT EXISTS ux_colloky_prueba_f_url_date
    ON {NOMBRE_TABLA} (product_url, date_scraped);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_date     ON {NOMBRE_TABLA} (date_scraped);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_sku      ON {NOMBRE_TABLA} (sku);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_type     ON {NOMBRE_TABLA} (product_type);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_material ON {NOMBRE_TABLA} (material)
"""

# Columns written by the scraper (order matters only for readability).
COLUMNAS_TABLA = [
    "date_scraped",
    "brand",
    "category",
    "page_number",
    "page_position",
    "category_rank",
    "sku",
    "sku_number",
    "product_url",
    "image_url",
    "product_name",
    "product_name_raw",
    "product_description",
    "description_main",
    "care_tips",
    "when_to_use",
    "pairs_with",
    "product_type",
    "gender",
    "age_segment",
    "age_min_months",
    "size_range",
    "is_licensed",
    "license_brand",
    "license_character",
    "material",
    "material_source",
    "material_composition",
    "color",
    "color_type",
    "color_count",
    "original_price",
    "final_price",
    "discount",
    "offers_total",
    "offers_in_stock",
    "offers_out_of_stock",
    "stockout_rate",
]

CLAVE_TABLA = {"product_url", "date_scraped"}


# =========================================================
# NORMALIZE TEXT
# =========================================================

def normalizar_texto(texto):

    if texto is None:
        return ""

    texto = html.unescape(str(texto)).strip()

    if not texto:
        return ""

    texto = unicodedata.normalize("NFD", texto.lower())

    texto = "".join(
        caracter
        for caracter in texto
        if unicodedata.category(caracter) != "Mn"
    )

    texto = re.sub(r"[^a-z0-9\s\-/]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto


def patron_palabra(palabra):
    """Regex that matches a whole word/phrase (no partial matches)."""
    return re.compile(
        r"(?<![a-z0-9])" + re.escape(palabra) + r"(?![a-z0-9])"
    )


def limpiar_html(texto):
    """Removes HTML tags and entities from a description and collapses spaces."""

    if not texto:
        return ""

    texto = html.unescape(str(texto))

    if "<" in texto and ">" in texto:
        from bs4 import BeautifulSoup
        texto = BeautifulSoup(texto, "html.parser").get_text(" ")

    return re.sub(r"\s+", " ", texto).strip()


# =========================================================
# DESCRIPTION SECTIONS
# =========================================================
# Headings that split the product description, as shown on the product
# page ("<h2>Consejos de cuidado</h2>"...). They are matched by TEXT
# (normalized: no accents, no "¿?", no ":"), never by CSS id.

SECCIONES_DESCRIPCION = ["description_main", "care_tips", "when_to_use", "pairs_with"]

ALIAS_SECCIONES = {
    "descripcion": "description_main",
    "descripcion del producto": "description_main",
    "consejos de cuidado": "care_tips",
    "instrucciones de cuidado": "care_tips",
    "cuidados": "care_tips",
    "cuidado de la prenda": "care_tips",
    "cuando usarlo": "when_to_use",
    "cuando usarla": "when_to_use",
    "cuando usar": "when_to_use",
    "prendas que combinan": "pairs_with",
    "combinalo con": "pairs_with",
    "combinala con": "pairs_with",
}


def detectar_encabezado(linea):
    """
    Returns (section_key, text_after_heading) when the line is a section
    heading, e.g. "Consejos de cuidado" or "Cuándo usarlo: en el colegio".
    Returns (None, "") otherwise.
    """
    izquierda, separador, derecha = linea.partition(":")

    candidatos = [(linea, "")]

    if separador:
        candidatos.append((izquierda, derecha))

    for candidato, resto in candidatos:
        clave = ALIAS_SECCIONES.get(normalizar_texto(candidato).strip(" -"))
        if clave:
            return clave, resto.strip()

    return None, ""


def separar_secciones_descripcion(descripcion_raw, especificaciones=None):
    """
    Splits a description (HTML or plain text) into its sections.

    Returns a dict with:
      product_description -> full clean text
      description_main, care_tips, when_to_use, pairs_with -> section text or None

    Text before the first heading goes to description_main.
    VTEX specifications named like a section are used when that section
    was not found in the description.
    """
    partes = {clave: [] for clave in SECCIONES_DESCRIPCION}
    todas_las_lineas = []

    texto = html.unescape(str(descripcion_raw)) if descripcion_raw else ""

    if "<" in texto and ">" in texto:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(texto, "html.parser")
        for salto in soup.find_all("br"):
            salto.replace_with("\n")
        # Every tag on its own line, so headings (<h2>, <strong>...) are isolated.
        texto = soup.get_text("\n")

    seccion_actual = "description_main"

    for linea in texto.splitlines():

        linea = re.sub(r"\s+", " ", linea).strip()

        if not linea:
            continue

        todas_las_lineas.append(linea)

        clave, resto = detectar_encabezado(linea)

        if clave:
            seccion_actual = clave
            if resto:
                partes[seccion_actual].append(resto)
            continue

        partes[seccion_actual].append(linea)

    resultado = {
        clave: (re.sub(r"\s+", " ", " ".join(lineas)).strip() or None)
        for clave, lineas in partes.items()
    }

    # Fallback: specifications named like a section.
    if isinstance(especificaciones, dict):
        for nombre, valor in especificaciones.items():
            clave = ALIAS_SECCIONES.get(normalizar_texto(nombre).strip(" -"))
            if clave and not resultado.get(clave) and valor:
                resultado[clave] = limpiar_html(valor) or None

    resultado["product_description"] = (
        re.sub(r"\s+", " ", " ".join(todas_las_lineas)).strip() or None
    )

    return resultado


# =========================================================
# PRODUCT TYPE CONFIGURATION
# =========================================================
#
# How the type is chosen: the keyword that appears EARLIEST in the
# product name wins. If two keywords start at the same position, the
# longest one wins ("pantalon buzo" beats "pantalon").
#
# "Pack" and "Set" are handled separately (see extraer_tipo_pack and
# extraer_tipo_set).
# =========================================================

TIPOS_PRODUCTO = [
    ("Outfit Muñeco", ["outfit muneco", "outfit de muneco", "outfit para muneco",
                       "ropa muneco", "ropa de muneco", "ropa para muneco"]),
    ("Saco de Dormir", ["saco de dormir", "sacos de dormir", "sleeping bag"]),
    ("Traje de Agua", ["traje de agua", "trajes de agua"]),
    ("Traje de Baño", ["traje de bano", "trajes de bano", "banador", "banadores", "swimsuit"]),
    ("Cubre Pañal", ["cubre panal", "cubrepanal", "cubre panales"]),
    ("Portabebe", ["portabebe", "portabebes", "porta bebe", "porta bebes", "porta-bebe"]),
    ("Portachupete", ["portachupete", "portachupetes", "porta chupete", "porta chupetes"]),
    ("Buzo", ["pantalon buzo", "pantalon de buzo", "pantalones de buzo", "buzo", "buzos"]),
    ("Cortaviento", ["cortaviento", "cortavientos", "corta viento", "corta vientos"]),
    ("Colonia", ["colonia", "colonias", "agua de colonia"]),
    ("Calceton", ["calceton", "calcetones"]),
    ("Calcetin", ["calcetin", "calcetines", "media", "medias", "soquete", "soquetes"]),
    ("Ballerina", ["ballerina", "ballerinas", "bailarina", "bailarinas", "baleta", "baletas"]),
    ("Mudador", ["mudador", "mudadores"]),
    ("Capota", ["capota", "capotas"]),
    ("Tuto", ["tuto", "tutos"]),
    ("Peto", ["peto", "petos"]),
    ("Suspensor", ["suspensor", "suspensores"]),
    ("Cuello", ["cuello", "cuellos"]),
    ("Panty", ["panty", "pantys", "panties"]),
    ("Pantina", ["pantina", "pantinas"]),
    ("Caja", ["caja", "cajas"]),
    ("Boina", ["boina", "boinas"]),
    ("Pata", ["pata", "patas"]),
    ("Pañal", ["panal", "panales"]),
    ("Disfraz", ["disfraz", "disfraces", "costume"]),
    ("Bombacho", ["bombacho", "bombachos"]),
    ("Calzon", ["calzon", "calzones", "calzoncillo", "calzoncillos",
                "bombacha", "bombachas", "braga", "bragas"]),
    ("Boxer", ["boxer", "boxers"]),
    ("Camiseta", ["camiseta", "camisetas", "camisero", "camiseros",
                  "t shirt", "t-shirt", "tshirt"]),
    ("Camisa", ["sobrecamisa", "sobre camisa", "camisa", "camisas"]),
    ("Blusa", ["blusa", "blusas"]),
    ("Polera", ["polera", "poleras"]),
    ("Polar", ["polar", "polares"]),
    ("Poleron", ["poleron", "polerones", "sudadera", "sudaderas", "hoodie", "hoodies"]),
    ("Sweater", ["sweater", "sweaters", "sueter", "sueters"]),
    ("Beatle", ["beatle", "beatles"]),
    ("Chaqueta", ["chaqueta", "chaquetas", "jacket", "jackets"]),
    ("Parka", ["parka", "parkas"]),
    ("Abrigo", ["abrigo", "abrigos"]),
    ("Capa", ["capa", "capas"]),
    ("Chaleco", ["chaleco", "chalecos"]),
    ("Vestido", ["vestido", "vestidos"]),
    ("Jumper", ["jumper", "jumpers"]),
    ("Falda", ["falda", "faldas"]),
    ("Jegging", ["jegging", "jeggings"]),
    ("Pantalon", ["pantalon", "pantalones", "jean", "jeans"]),
    ("Calza", ["calza", "calzas", "legging", "leggings"]),
    ("Short", ["short", "shorts", "bermuda", "bermudas"]),
    ("Conjunto", ["conjunto", "conjuntos"]),
    ("Enterito", ["enterito", "enteritos", "entero", "enteros", "overol", "overoles",
                  "jardinera", "jardineras"]),
    ("Body", ["body", "bodys", "bodies"]),
    ("Pilucho", ["pilucho", "piluchos"]),
    ("Mameluco", ["mameluco", "mamelucos"]),
    ("Osito", ["osito", "ositos"]),
    ("Oso", ["oso", "osos"]),
    ("Pijama", ["pijama", "pijamas"]),
    ("Bata", ["bata", "batas"]),
    ("Bikini", ["bikini", "bikinis"]),
    ("Tankini", ["tankini", "tankinis"]),
    ("Zapatilla", ["zapatilla", "zapatillas", "sneaker", "sneakers"]),
    ("Zapato", ["zapato", "zapatos"]),
    ("Bootie", ["bootie", "booties"]),
    ("Botin", ["botin", "botines"]),
    ("Bota", ["bota", "botas"]),
    ("Sandalia", ["sandalia", "sandalias"]),
    ("Alpargata", ["alpargata", "alpargatas"]),
    ("Hawaiana", ["hawaiana", "hawaianas"]),
    ("Mocasin", ["mocasin", "mocasines"]),
    ("Reina", ["reina", "reinas"]),
    ("Aqua", ["aqua", "aquas"]),
    ("Pantufla", ["pantufla", "pantuflas"]),
    ("Babucha", ["babucha", "babuchas"]),
    ("Mochila", ["mochila", "mochilas"]),
    ("Bolso", ["bolso", "bolsos", "bolsa", "bolsas", "cartera", "carteras"]),
    ("Banano", ["banano", "bananos", "rinonera", "rinoneras"]),
    ("Lonchera", ["lonchera", "loncheras"]),
    ("Estuche", ["estuche", "estuches"]),
    ("Billetera", ["billetera", "billeteras"]),
    ("Neceser", ["neceser", "neceseres", "cosmetiquero", "cosmetiqueros"]),
    ("Anteojos", ["anteojo", "anteojos", "lente", "lentes", "gafa", "gafas"]),
    ("Gorro", ["gorro", "gorros", "beanie", "beanies"]),
    ("Sombrero", ["sombrero", "sombreros"]),
    ("Jockey", ["jockey", "jockeys", "gorra", "gorras"]),
    ("Bufanda", ["bufanda", "bufandas"]),
    ("Guante", ["guante", "guantes", "miton", "mitones"]),
    ("Cinturon", ["cinturon", "cinturones"]),
    ("Corbata", ["corbata", "corbatas"]),
    ("Cintillo", ["cintillo", "cintillos", "diadema", "diademas"]),
    ("Cole", ["cole", "coles", "colet", "colets", "scrunchie", "scrunchies"]),
    ("Pinche", ["pinche", "pinches", "hebilla", "hebillas"]),
    ("Collar", ["collar", "collares"]),
    ("Pulsera", ["pulsera", "pulseras"]),
    ("Aro", ["aro", "aros", "pendiente", "pendientes"]),
    ("Reloj", ["reloj", "relojes"]),
    ("Babita", ["babita", "babitas"]),
    ("Babero", ["babero", "baberos"]),
    ("Chupete", ["chupete", "chupetes"]),
    ("Mamadera", ["mamadera", "mamaderas", "biberon", "biberones"]),
    ("Mordedor", ["mordedor", "mordedores"]),
    ("Sonajero", ["sonajero", "sonajeros"]),
    ("Juguete", ["juguete", "juguetes", "muneca", "munecas", "muneco", "munecos",
                 "peluche", "peluches", "figura", "figuras"]),
    ("Manta", ["manta", "mantas", "frazada", "frazadas"]),
    ("Toallon", ["toallon", "toallones"]),
    ("Toalla", ["toalla", "toallas"]),
    ("Vaso", ["vaso", "vasos"]),
    ("Botella", ["botella", "botellas"]),
    ("Plato", ["plato", "platos"]),
    ("Cubierto", ["cubierto", "cubiertos", "cuchara", "cucharas", "tenedor", "tenedores"]),
    ("Paraguas", ["paraguas"]),
    ("Silla", ["silla", "sillas"]),
    ("Coche", ["coche", "coches"]),
    ("Andador", ["andador", "andadores"]),
    ("Alfombra", ["alfombra", "alfombras"]),
    ("Protector", ["protector", "protectores"]),
    ("Ajuar", ["ajuar", "ajuares"]),
    ("Accesorio", ["accesorio", "accesorios"]),
]

# Footwear and socks: their sizes are shoe sizes (14-18, 21-27, 28-38...).
TIPOS_CON_TALLA_CALZADO = {
    "Zapatilla", "Zapato", "Bootie", "Botin", "Bota", "Sandalia", "Alpargata",
    "Hawaiana", "Mocasin", "Reina", "Aqua", "Pantufla", "Babucha", "Ballerina",
    "Pantina", "Calcetin", "Calceton",
}


def preparar_patrones_productos():

    patrones = []

    for categoria, palabras_clave in TIPOS_PRODUCTO:
        for palabra_clave in palabras_clave:
            palabra_normalizada = normalizar_texto(palabra_clave)
            patrones.append(
                {
                    "categoria": categoria,
                    "palabra_clave": palabra_normalizada,
                    "patron": patron_palabra(palabra_normalizada),
                }
            )

    return patrones


PATRONES_PRODUCTOS = preparar_patrones_productos()


def primera_coincidencia(texto, excluir=None):
    """
    Returns (category, position) for the keyword that appears earliest
    in the text. Ties are broken by the longest keyword.
    Returns None when nothing matches.
    """
    excluir = excluir or set()
    coincidencias = []

    for dato in PATRONES_PRODUCTOS:

        if dato["categoria"] in excluir:
            continue

        coincidencia = dato["patron"].search(texto)

        if coincidencia:
            coincidencias.append(
                (coincidencia.start(), -len(dato["palabra_clave"]), dato["categoria"])
            )

    if not coincidencias:
        return None

    coincidencias.sort()
    posicion, _, categoria = coincidencias[0]

    return categoria, posicion


# =========================================================
# TYPO CORRECTION
# =========================================================

# Known typos in the store. Applied before any classification.
ERRORES_PRODUCTO_EXPLICITOS = {
    "calcetq": "calcetin",
    "calcet": "calcetin",
    "calcetins": "calcetin",
    "calcetom": "calceton",
    "calcetoms": "calceton",
    "chupata": "chupete",
    "chupatas": "chupete",
    "chupetas": "chupete",
    "porta bebe": "portabebe",
    "porta bebes": "portabebe",
    "porta-bebe": "portabebe",
    "porta-bebes": "portabebe",
    "bombracho": "bombacho",
    "jeggins": "jegging",
    "nisex": "unisex",
    "marngas": "mangas",
    "gaardina": "gabardina",
    "viscera": "visera",
    "collokyde": "colloky de",
}

PATRONES_ERRORES = [
    (patron_palabra(normalizar_texto(error)), correcto)
    for error, correcto in sorted(
        ERRORES_PRODUCTO_EXPLICITOS.items(),
        key=lambda elemento: len(elemento[0]),
        reverse=True,
    )
]


# =========================================================
# COLOR CONFIGURATION
# =========================================================

COLORES_BASE = {
    "amarillo", "amarilla", "arena", "azul", "beige", "bicolor", "blanco", "blanca",
    "bronce", "burdeo", "cafe", "calipso", "camel", "caqui", "celeste", "chicle",
    "cobre", "coral", "crema", "crudo", "cruda", "damasco", "denim", "dorado",
    "dorada", "fucsia", "grafito", "gris", "khaki", "lavanda", "lila", "lima",
    "magenta", "marengo", "marron", "menta", "mora", "morado", "morada", "mostaza",
    "multicolor", "naranja", "naranjo", "negro", "negra", "nude", "oliva",
    "petroleo", "plateado", "plateada", "purpura", "rojo", "roja", "rosa", "rosado",
    "rosada", "salmon", "terracota", "turquesa", "verde", "violeta",
}

# Used when the color appears on its own.
NORMALIZACION_COLORES = {
    "amarilla": "Amarillo",
    "blanca": "Blanco",
    "caqui": "Khaki",
    "cruda": "Crudo",
    "dorada": "Dorado",
    "morada": "Morado",
    "naranjo": "Naranja",
    "negra": "Negro",
    "plateada": "Plateado",
    "roja": "Rojo",
    "rosa": "Rosado",
    "rosada": "Rosado",
}

# Used when the color is followed by a shade ("rosa viejo" stays "Rosa Viejo").
NORMALIZACION_COLORES_CON_MATIZ = {
    "rosa": "Rosa",
}

MATICES_COLOR = {
    "azul": {"marino", "rey", "electrico", "petroleo", "francia", "cielo", "oscuro", "claro"},
    "verde": {"militar", "oliva", "musgo", "menta", "agua", "oscuro", "claro", "neon"},
    "gris": {"melange", "oscuro", "claro", "perla", "oxford"},
    "rosa": {"palo", "chicle", "fuerte", "claro", "oscuro", "viejo"},
    "rosado": {"palo", "chicle", "claro", "oscuro"},
    "rojo": {"vino", "oscuro", "claro"},
    "cafe": {"claro", "oscuro"},
    "marron": {"claro", "oscuro"},
    "beige": {"claro", "oscuro"},
    "morado": {"claro", "oscuro"},
    "lila": {"claro", "oscuro"},
    "amarillo": {"claro", "oscuro"},
    "naranja": {"claro", "oscuro"},
    "naranjo": {"claro", "oscuro"},
    "blanco": {"hueso", "optico", "marfil"},
    "blanca": {"hueso", "optico", "marfil"},
    "negro": {"carbon", "oscuro"},
    "negra": {"carbon", "oscuro"},
}


# =========================================================
# MATERIAL CONFIGURATION
# =========================================================

MATERIALES = [
    ("Faux Leather", ["ecocuero", "eco cuero", "cuero sintetico"]),
    ("Patent Leather", ["charol"]),
    ("Leather", ["cuero"]),
    ("Polycotton", ["polycotton", "policotton"]),
    ("Cotton", ["algodon"]),
    ("Wool", ["lana"]),
    ("Denim", ["mezclilla", "denim", "jeans", "jean"]),
    ("Gabardine", ["gabardina"]),
    ("Linen", ["lino"]),
    ("Viscose", ["viscosa"]),
    ("Canvas", ["lona"]),
    ("Bambula", ["bambula"]),
    ("Rib Knit", ["rib"]),
    ("Plush", ["plush"]),
    ("Sherpa", ["chiporro", "sherpa", "sherpas"]),
    ("Fleece", ["polar", "polar fleece"]),
    ("Flannel", ["franela"]),
    ("Corduroy", ["cotele"]),
    ("Knit", ["tejido", "tejida", "tejidos", "tejidas"]),
    ("Broderie", ["broderie", "brodery"]),
    ("Poplin", ["popelina"]),
    ("Raffia", ["rafia"]),
    ("Cork", ["corcho"]),
    ("Suede", ["gamuza"]),
    ("EVA", ["eva"]),
    ("Microfiber", ["microfibra"]),
    ("Velvet", ["velvet", "terciopelo"]),
    ("Jersey", ["jersey"]),
    ("Lycra", ["lycra"]),
    ("Chenille", ["chenille"]),
    ("Mesh", ["mesh"]),
    ("Stainless Steel", ["acero inoxidable"]),
    ("Wood", ["madera"]),
    # Typical of the "Composición" specification:
    ("Polyester", ["poliester", "polyester"]),
    ("Elastane", ["elastano", "spandex", "elastane"]),
    ("Nylon", ["nylon", "nailon", "poliamida"]),
    ("Acrylic", ["acrilico"]),
    ("Rubber", ["goma", "caucho"]),
    ("PVC", ["pvc"]),
    ("TPR", ["tpr"]),
    ("Synthetic", ["sintetico", "sintetica"]),
]

# These materials are only accepted when they come from the composition:
# in names/descriptions "goma", "sintetico", etc. are too ambiguous
# ("calcetín antideslizante con goma", "cuero sintético" -> already Faux Leather).
MATERIALES_SOLO_COMPOSICION = {"Rubber", "PVC", "TPR", "Synthetic"}

# (material, [(keyword, regex), ...])
PATRONES_MATERIALES = [
    (
        material,
        [
            (normalizar_texto(palabra), patron_palabra(normalizar_texto(palabra)))
            for palabra in palabras
        ],
    )
    for material, palabras in MATERIALES
]

# In the marketing description these words are ambiguous, so they are ignored:
#  - "jean/jeans": "combina perfecto con tus jeans" does not mean it is denim.
#  - "tejido/tejida": in descriptions it usually means "fabric", not "knit".
PALABRAS_EXCLUIDAS_EN_DESCRIPCION = {"jean", "jeans"}
MATERIALES_EXCLUIDOS_EN_DESCRIPCION = {"Knit"}

# VTEX specification names that may contain the material.
PATRON_ESPECIFICACION_MATERIAL = re.compile(r"materia|composic|tela|tejido|fibra")


# =========================================================
# LICENSE CONFIGURATION
# =========================================================
# (keyword, character / franchise, license owner)
# Generic keywords ("disney", "marvel") are only used when no specific
# character is found.

LICENCIAS = [
    ("lilo stitch", "Lilo & Stitch", "Disney"),
    ("stitch", "Lilo & Stitch", "Disney"),
    ("spider man", "Spider-Man", "Marvel"),
    ("spiderman", "Spider-Man", "Marvel"),
    ("spidey", "Spidey", "Marvel"),
    ("avengers", "Avengers", "Marvel"),
    ("iron man", "Iron Man", "Marvel"),
    ("minnie", "Minnie Mouse", "Disney"),
    ("mickey", "Mickey Mouse", "Disney"),
    ("daisy", "Daisy Duck", "Disney"),
    ("pato donald", "Donald Duck", "Disney"),
    ("winnie the pooh", "Winnie the Pooh", "Disney"),
    ("toy story", "Toy Story", "Disney"),
    ("frozen", "Frozen", "Disney"),
    ("cars", "Cars", "Disney"),
    ("dumbo", "Dumbo", "Disney"),
    ("rey leon", "The Lion King", "Disney"),
    ("moana", "Moana", "Disney"),
    ("libro de la selva", "The Jungle Book", "Disney"),
    ("marie", "Marie (Aristocats)", "Disney"),
]

LICENCIAS_GENERICAS = [
    ("disney", "Disney (other)", "Disney"),
    ("marvel", "Marvel (other)", "Marvel"),
]

PATRONES_LICENCIAS = [
    (patron_palabra(palabra), personaje, dueno)
    for palabra, personaje, dueno in LICENCIAS
]

PATRONES_LICENCIAS_GENERICAS = [
    (patron_palabra(palabra), personaje, dueno)
    for palabra, personaje, dueno in LICENCIAS_GENERICAS
]


# =========================================================
# FUZZY TYPO CORRECTION
# =========================================================
# Candidates: single-word product keywords with 6+ letters.
# Protected words (colors, materials, licenses and common descriptive
# words) are never "corrected".

CANDIDATOS_FUZZY = {
    normalizar_texto(palabra)
    for _, palabras in TIPOS_PRODUCTO
    for palabra in palabras
    if " " not in palabra and len(normalizar_texto(palabra)) >= 6
}

PALABRAS_PROTEGIDAS = set(CANDIDATOS_FUZZY)
PALABRAS_PROTEGIDAS.update(COLORES_BASE)
PALABRAS_PROTEGIDAS.update(
    normalizar_texto(palabra)
    for _, palabras in MATERIALES
    for frase in palabras
    for palabra in frase.split()
)
PALABRAS_PROTEGIDAS.update(
    palabra
    for frase, _, _ in LICENCIAS + LICENCIAS_GENERICAS
    for palabra in frase.split()
)
PALABRAS_PROTEGIDAS.update({
    "boton", "botones", "bolsillo", "bolsillos", "cintura", "mangas", "coleccion",
    "estampado", "estampada", "elasticado", "elasticada", "unisex", "flores",
    "rayas", "cuadros", "bordado", "bordada", "cierre", "velcro", "cordon",
})


def corregir_errores_producto(texto):
    """
    Normalizes the name and fixes known typos.

    Fuzzy matching is deliberately strict:
    - only words with 6+ letters;
    - protected words are never touched;
    - similarity >= 0.80, same first 3 letters, clear margin over the
      second-best candidate.
    """
    texto_normalizado = normalizar_texto(texto)

    if not texto_normalizado:
        return ""

    for patron, correcto in PATRONES_ERRORES:
        texto_normalizado = patron.sub(correcto, texto_normalizado)

    palabras_corregidas = []

    for palabra in texto_normalizado.split():

        if (
            len(palabra) < 6
            or palabra in PALABRAS_PROTEGIDAS
            or not palabra.isalpha()
        ):
            palabras_corregidas.append(palabra)
            continue

        candidatos = [
            candidato
            for candidato in CANDIDATOS_FUZZY
            if candidato[:3] == palabra[:3]
            and abs(len(candidato) - len(palabra)) <= 3
        ]

        if not candidatos:
            palabras_corregidas.append(palabra)
            continue

        puntuaciones = sorted(
            (
                (SequenceMatcher(None, palabra, candidato).ratio(), candidato)
                for candidato in candidatos
            ),
            reverse=True,
        )

        mejor_ratio, mejor_candidato = puntuaciones[0]
        segundo_ratio = puntuaciones[1][0] if len(puntuaciones) > 1 else 0

        if mejor_ratio >= 0.80 and mejor_ratio - segundo_ratio >= 0.05:
            palabras_corregidas.append(mejor_candidato)
        else:
            palabras_corregidas.append(palabra)

    return " ".join(palabras_corregidas)


# =========================================================
# SET PRODUCTS
# =========================================================
# Only these words are accepted after "Set". The one that appears first
# wins ("Set body cuello rib" -> Set Body).

TIPOS_PERMITIDOS_DESPUES_DE_SET = {
    "clinica": "Clinica", "hospital": "Clinica", "escolar": "Escolar",
    "recien nacido": "Recien Nacido", "newborn": "Recien Nacido",
    "accesorio": "Accesorio", "accesorios": "Accesorio",
    "bootie": "Bootie", "alpargata": "Alpargata", "aqua": "Aqua",
    "beatle": "Beatle", "bombacho": "Bombacho", "buzo": "Buzo",
    "conjunto": "Conjunto", "cortaviento": "Cortaviento", "hawaiana": "Hawaiana",
    "jegging": "Jegging", "mocasin": "Mocasin", "oso": "Oso", "reina": "Reina",
    "tankini": "Tankini", "traje de agua": "Traje de Agua",
    "gorro": "Gorro", "gorros": "Gorro",
    "suspensor": "Suspensor", "suspensores": "Suspensor",
    "calcetin": "Calcetin", "calcetines": "Calcetin",
    "calceton": "Calceton", "calcetones": "Calceton",
    "body": "Body", "bodys": "Body", "bodies": "Body",
    "pijama": "Pijama", "pijamas": "Pijama",
    "polera": "Polera", "poleras": "Polera",
    "poleron": "Poleron", "polerones": "Poleron",
    "camiseta": "Camiseta", "camisetas": "Camiseta",
    "pantalon": "Pantalon", "pantalones": "Pantalon",
    "calza": "Calza", "calzas": "Calza",
    "short": "Short", "shorts": "Short",
    "babero": "Babero", "baberos": "Babero",
    "tuto": "Tuto", "tutos": "Tuto",
    "panal": "Pañal", "panales": "Pañal",
    "chupete": "Chupete", "chupetes": "Chupete",
    "portachupete": "Portachupete", "portabebe": "Portabebe",
    "mudador": "Mudador", "mudadores": "Mudador",
    "peto": "Peto", "petos": "Peto",
    "cuello": "Cuello", "cuellos": "Cuello",
    "panty": "Panty", "pantys": "Panty",
    "boina": "Boina", "boinas": "Boina",
    "capota": "Capota", "capotas": "Capota",
    "guante": "Guante", "guantes": "Guante",
    "cintillo": "Cintillo", "cintillos": "Cintillo",
    "cole": "Cole", "coles": "Cole",
    "pinche": "Pinche", "pinches": "Pinche",
    "jockey": "Jockey", "jockeys": "Jockey",
    "mochila": "Mochila", "mochilas": "Mochila",
    "bolso": "Bolso", "bolsos": "Bolso",
    "estuche": "Estuche", "estuches": "Estuche",
    "colonia": "Colonia", "colonias": "Colonia",
    "ballerina": "Ballerina", "ballerinas": "Ballerina",
    "bailarina": "Ballerina", "bailarinas": "Ballerina",
    "disfraz": "Disfraz", "disfraces": "Disfraz",
    "juguete": "Juguete", "juguetes": "Juguete",
    "mordedor": "Mordedor", "mordedores": "Mordedor",
    "sonajero": "Sonajero", "sonajeros": "Sonajero",
    "mamadera": "Mamadera", "mamaderas": "Mamadera",
    "toalla": "Toalla", "toallas": "Toalla",
    "toallon": "Toallon", "toallones": "Toallon",
    "manta": "Manta", "mantas": "Manta",
    "caja": "Caja", "cajas": "Caja",
    "ropa": "Ropa", "outfit": "Outfit",
}

PATRONES_SET = [
    (patron_palabra(clave), categoria, len(clave))
    for clave, categoria in TIPOS_PERMITIDOS_DESPUES_DE_SET.items()
]

PATRON_PACK = re.compile(r"\bpacks?\b")
PATRON_SET = re.compile(r"\bsets?\b")


def extraer_tipo_set(nombre_normalizado):

    coincidencia_set = PATRON_SET.search(nombre_normalizado)

    if not coincidencia_set:
        return None

    texto = nombre_normalizado[coincidencia_set.end():].strip()
    texto = re.sub(r"^(?:de|para|con)\s+", "", texto).strip()

    coincidencias = []

    for patron, categoria, largo in PATRONES_SET:
        coincidencia = patron.search(texto)
        if coincidencia:
            coincidencias.append((coincidencia.start(), -largo, categoria))

    if not coincidencias:
        return "Set"

    coincidencias.sort()

    return f"Set {coincidencias[0][2]}"


# =========================================================
# PACK PRODUCTS
# =========================================================

def extraer_tipo_pack(nombre_normalizado):
    """
    "Pack 3 grises Calcetines" -> Pack Calcetin
    "Pack 2 pijamas"           -> Pack Pijama
    Unknown content            -> Pack
    """
    coincidencia_pack = PATRON_PACK.search(nombre_normalizado)

    if not coincidencia_pack:
        return None

    texto = nombre_normalizado[coincidencia_pack.end():].strip()

    resultado = primera_coincidencia(texto, excluir={"Accesorio"})

    if not resultado:
        return "Pack"

    return f"Pack {resultado[0]}"


NOMBRES_PACK_LIMPIOS = {
    "Calcetin": "Calcetines", "Calceton": "Calcetones", "Pijama": "Pijamas",
    "Body": "Bodies", "Camiseta": "Camisetas", "Polera": "Poleras",
    "Pantalon": "Pantalones", "Calza": "Calzas", "Short": "Shorts",
    "Babero": "Baberos", "Tuto": "Tutos", "Pañal": "Pañales",
    "Chupete": "Chupetes", "Mamadera": "Mamaderas", "Manta": "Mantas",
    "Toalla": "Toallas", "Toallon": "Toallones", "Gorro": "Gorros",
    "Guante": "Guantes", "Juguete": "Juguetes", "Mudador": "Mudadores",
}


def limpiar_nombre_producto(product_name):
    """
    Pack names are standardised ("Pack 3 grises Calcetines" -> "Pack Calcetines").
    Other names only get HTML entities decoded and extra spaces removed.
    The original name is always kept in product_name_raw.
    """
    if not product_name:
        return ""

    nombre_original = re.sub(r"\s+", " ", html.unescape(str(product_name))).strip()
    nombre_normalizado = corregir_errores_producto(nombre_original)

    if not PATRON_PACK.search(nombre_normalizado):
        return nombre_original

    tipo_pack = extraer_tipo_pack(nombre_normalizado)

    if not tipo_pack or tipo_pack == "Pack":
        return "Pack"

    categoria = tipo_pack.replace("Pack ", "", 1)

    return f"Pack {NOMBRES_PACK_LIMPIOS.get(categoria, categoria)}"


# =========================================================
# PRODUCT TYPE
# =========================================================

def extraer_tipo_producto(product_name):

    nombre = corregir_errores_producto(product_name)

    if not nombre:
        return "Other"

    resultado = primera_coincidencia(nombre)
    posicion_tipo = resultado[1] if resultado else len(nombre) + 1

    # Pack / Set only win when they appear before any regular product type.
    coincidencia_pack = PATRON_PACK.search(nombre)

    if coincidencia_pack and coincidencia_pack.start() <= posicion_tipo:
        return extraer_tipo_pack(nombre)

    coincidencia_set = PATRON_SET.search(nombre)

    if coincidencia_set and coincidencia_set.start() <= posicion_tipo:
        return extraer_tipo_set(nombre)

    if resultado:
        return resultado[0]

    return "Other"


# =========================================================
# COLORS
# =========================================================

def color_base(palabra):
    """Matches plurals too: 'grises' -> 'gris', 'rosadas' -> 'rosada'."""

    if palabra in COLORES_BASE:
        return palabra

    if palabra.endswith("es") and palabra[:-2] in COLORES_BASE:
        return palabra[:-2]

    if palabra.endswith("s") and palabra[:-1] in COLORES_BASE:
        return palabra[:-1]

    return None


def normalizar_color_individual(base, matiz=None):

    if matiz is None:
        return NORMALIZACION_COLORES.get(base, base.title())

    base_texto = NORMALIZACION_COLORES_CON_MATIZ.get(
        base,
        NORMALIZACION_COLORES.get(base, base.title()),
    )

    return f"{base_texto} {matiz.title()}"


def extraer_colores(product_name):
    """Returns the list of colors found in the name, without duplicates."""

    palabras = re.findall(r"[a-z]+", normalizar_texto(product_name))
    colores = []
    posicion = 0

    while posicion < len(palabras):

        palabra = palabras[posicion]
        siguiente = palabras[posicion + 1] if posicion + 1 < len(palabras) else ""

        if palabra == "off" and siguiente == "white":
            colores.append("Off White")
            posicion += 2
            continue

        base = color_base(palabra)

        if not base:
            posicion += 1
            continue

        if siguiente in MATICES_COLOR.get(base, set()):
            colores.append(normalizar_color_individual(base, siguiente))
            posicion += 2
            continue

        colores.append(normalizar_color_individual(base))
        posicion += 1

    return list(dict.fromkeys(colores))


def resumir_colores(colores):
    """
    Returns (color, color_type, color_count).
    "Multicolor" and "Bicolor" always count as at least 2 colors.
    """
    if not colores:
        return "Color Not Specified", "Color Not Specified", 0

    texto = ", ".join(colores)

    if "Multicolor" in colores or "Bicolor" in colores:
        return texto, "Multi", max(2, len(colores))

    if len(colores) == 1:
        return texto, "Single", 1

    return texto, "Multi", len(colores)


# =========================================================
# GENDER
# =========================================================

def extraer_genero(nombre_normalizado):

    palabras = set(re.findall(r"[a-z]+", nombre_normalizado))

    es_nina = bool(palabras & {"nina", "ninas"})
    es_nino = bool(palabras & {"nino", "ninos"})

    if "unisex" in palabras or (es_nina and es_nino):
        return "Unisex"

    if es_nina:
        return "Girl"

    if es_nino:
        return "Boy"

    return "Not Specified"


# =========================================================
# AGE SEGMENT AND SIZE RANGE
# =========================================================
#
# The starting age of the size range is used:
#   "6/9M a 4/5A"   -> 6 months  -> Baby
#   "3/4A a 12/13A" -> 3 years   -> Kids
#   "RN a 6-9M"     -> 0 months  -> Baby
# Footwear uses EU shoe sizes:
#   14-17 -> Baby, 18-27 -> Toddler, 28+ -> Kids
# =========================================================

UNIDAD_EDAD = r"(meses|mes|m|anos|ano|a)"

PATRON_RN = re.compile(r"\b(rn|new born|newborn|recien nacido)\b")

# "3 a 36 meses", "2 a 12 anos", "6-9m", "1/2 a 3/4 anos"
PATRON_RANGO_EDAD = re.compile(
    r"\b(\d{1,2})(?:\s*/\s*\d{1,2})?\s*(?:a|al|-)\s*\d{1,2}(?:\s*/\s*\d{1,2})?\s*"
    + UNIDAD_EDAD + r"\b"
)

# "0/3m", "12 meses", "18/24m"
PATRON_MESES = re.compile(
    r"\b(\d{1,2})(?:\s*[/-]\s*\d{1,2})?\s*(meses|mes|m)\b"
)

# "2a", "3/4a", "10 anos" (the single letter "a" must be attached to the
# number, otherwise shoe sizes like "28 a 38" would be read as years)
PATRON_ANOS = re.compile(
    r"\b(\d{1,2})(?:\s*[/-]\s*\d{1,2})?(?:\s*(anos|ano)|(a))\b"
)

PATRON_TALLA_CALZADO = re.compile(
    r"\b(1[4-9]|[2-4]\d)\s*(?:-|a|al)\s*(1[5-9]|[2-4]\d)\b"
)

PATRON_TALLA_UNICA = re.compile(r"\btalla unica\b|\btalle unico\b|\btu$")


def a_meses(valor, unidad):

    valor = int(valor)

    return valor * 12 if unidad and unidad.startswith("a") else valor


def segmento_por_meses(meses):

    if meses < 12:
        return "Baby (0-11M)"

    if meses < 36:
        return "Toddler (1-2Y)"

    return "Kids (3Y+)"


def extraer_segmento_edad(nombre_normalizado, product_type, categoria):
    """Returns (age_segment, age_min_months, size_range)."""

    candidatos = []

    for coincidencia in PATRON_RN.finditer(nombre_normalizado):
        candidatos.append((coincidencia.start(), 0, coincidencia.group(0)))

    for coincidencia in PATRON_RANGO_EDAD.finditer(nombre_normalizado):
        candidatos.append(
            (
                coincidencia.start(),
                a_meses(coincidencia.group(1), coincidencia.group(2)),
                coincidencia.group(0),
            )
        )

    for coincidencia in PATRON_MESES.finditer(nombre_normalizado):
        candidatos.append(
            (coincidencia.start(), int(coincidencia.group(1)), coincidencia.group(0))
        )

    for coincidencia in PATRON_ANOS.finditer(nombre_normalizado):
        unidad = coincidencia.group(2) or coincidencia.group(3)
        candidatos.append(
            (
                coincidencia.start(),
                a_meses(coincidencia.group(1), unidad),
                coincidencia.group(0),
            )
        )

    if candidatos:
        candidatos.sort(key=lambda dato: (dato[0], -len(dato[2])))
        _, meses, texto = candidatos[0]

        # Keep the full range text when there is one ("6/9m a 4/5a").
        rango = re.search(
            re.escape(texto) + r"(?:\s*(?:a|al|-)\s*[\d/\- ]+\s*" + UNIDAD_EDAD + r"\b)?",
            nombre_normalizado,
        )

        return segmento_por_meses(meses), meses, (rango.group(0) if rango else texto).strip()

    if product_type in TIPOS_CON_TALLA_CALZADO or categoria == "Footwear":

        coincidencia = PATRON_TALLA_CALZADO.search(nombre_normalizado)

        if coincidencia:
            talla_min = int(coincidencia.group(1))

            if talla_min <= 17:
                segmento = "Baby (0-11M)"
            elif talla_min <= 27:
                segmento = "Toddler (1-2Y)"
            else:
                segmento = "Kids (3Y+)"

            return segmento, None, f"EU {coincidencia.group(1)}-{coincidencia.group(2)}"

    if PATRON_TALLA_UNICA.search(nombre_normalizado):
        return "One Size", None, "talla unica"

    return "Not Specified", None, None


# =========================================================
# LICENSE
# =========================================================

def extraer_licencia(nombre_normalizado):
    """Returns (is_licensed, license_brand, license_character)."""

    for patrones in (PATRONES_LICENCIAS, PATRONES_LICENCIAS_GENERICAS):

        coincidencias = []

        for patron, personaje, dueno in patrones:
            coincidencia = patron.search(nombre_normalizado)
            if coincidencia:
                coincidencias.append((coincidencia.start(), -len(patron.pattern), personaje, dueno))

        if coincidencias:
            coincidencias.sort()
            _, _, personaje, dueno = coincidencias[0]
            return True, dueno, personaje

    return False, "Own Brand", "None"


# =========================================================
# MATERIAL
# =========================================================

def buscar_materiales(texto_normalizado, modo="name"):
    """
    Returns the list of materials found in an already-normalized text.
    modo:
      "composition" -> every material is accepted (most precise source)
      "name"        -> composition-only materials are skipped
      "description" -> composition-only materials and ambiguous words are skipped
    """
    encontrados = []

    if not texto_normalizado:
        return encontrados

    for material, palabras in PATRONES_MATERIALES:

        if modo != "composition" and material in MATERIALES_SOLO_COMPOSICION:
            continue

        if modo == "description" and material in MATERIALES_EXCLUIDOS_EN_DESCRIPCION:
            continue

        for palabra, patron in palabras:

            if modo == "description" and palabra in PALABRAS_EXCLUIDAS_EN_DESCRIPCION:
                continue

            if patron.search(texto_normalizado):
                encontrados.append(material)
                break

    if "Faux Leather" in encontrados:
        for redundante in ("Leather", "Synthetic"):
            if redundante in encontrados:
                encontrados.remove(redundante)

    return encontrados


def texto_especificaciones_material(especificaciones):
    """
    Joins the values of the VTEX specifications that look like material
    fields ("Composición", "Material", "Tela"...), as published.
    Example: {"Composición": "100% Algodón."} -> "100% Algodón."
    """
    if not isinstance(especificaciones, dict):
        return ""

    valores = [
        re.sub(r"\s+", " ", limpiar_html(valor)).strip()
        for nombre, valor in especificaciones.items()
        if valor and PATRON_ESPECIFICACION_MATERIAL.search(normalizar_texto(nombre))
    ]

    return " | ".join(valor for valor in valores if valor)


def extraer_material(nombre_normalizado, descripcion=None, especificaciones=None):
    """
    Returns (material, material_source, material_composition).

    Priority:
      1. Composition specification. If it yields a material, it is the
         only source used (it is the most precise one).
      2. Otherwise, product name + description combined.
    The raw composition text is always returned (even when no known
    material is found in it), so it can be reviewed in SQL.
    """
    composicion = texto_especificaciones_material(especificaciones)

    # ---------- 1. Composition ----------
    desde_composicion = buscar_materiales(normalizar_texto(composicion), modo="composition")

    if desde_composicion:
        return ", ".join(desde_composicion), "specification", composicion or None

    # ---------- 2. Name + description ----------
    materiales = []
    fuentes = []

    def agregar(lista, fuente):
        if lista:
            fuentes.append(fuente)
            for material in lista:
                if material not in materiales:
                    materiales.append(material)

    agregar(buscar_materiales(nombre_normalizado, modo="name"), "name")
    agregar(buscar_materiales(normalizar_texto(descripcion), modo="description"), "description")

    if "Faux Leather" in materiales and "Leather" in materiales:
        materiales.remove("Leather")

    if not materiales:
        return "Not Specified", "Not Specified", composicion or None

    return ", ".join(materiales), "+".join(fuentes), composicion or None


# =========================================================
# CLASSIFY PRODUCT (single entry point)
# =========================================================

def clasificar_producto(product_name_raw, categoria=None, descripcion=None, especificaciones=None):

    nombre_normalizado = corregir_errores_producto(product_name_raw)
    product_type = extraer_tipo_producto(product_name_raw)
    color, color_type, color_count = resumir_colores(extraer_colores(product_name_raw))
    age_segment, age_min_months, size_range = extraer_segmento_edad(
        nombre_normalizado, product_type, categoria
    )
    is_licensed, license_brand, license_character = extraer_licencia(nombre_normalizado)
    material, material_source, material_composition = extraer_material(
        nombre_normalizado, descripcion, especificaciones
    )

    return {
        "product_type": product_type,
        "gender": extraer_genero(nombre_normalizado),
        "age_segment": age_segment,
        "age_min_months": age_min_months,
        "size_range": size_range,
        "is_licensed": is_licensed,
        "license_brand": license_brand,
        "license_character": license_character,
        "material": material,
        "material_source": material_source,
        "material_composition": material_composition,
        "color": color,
        "color_type": color_type,
        "color_count": color_count,
    }


# =========================================================
# TEST CLASSIFICATION
# =========================================================

DESCRIPCION_EJEMPLO = (
    "Descubre la pijama Colloky temática de dormir para niños en color negro, "
    "una prenda que combina confort y estilo a la perfección. Confeccionada en "
    "100% algodón, garantiza suavidad y transpirabilidad para un descanso óptimo. "
    "El diseño camisero clásico y elegante les brinda a los pequeños comodidad y "
    "moda al mismo tiempo."
)


def probar_clasificacion():

    # (name, category, description)
    ejemplos = [
        ("Set Gorro Colección Invierno", "Accessories", None),
        ("Set body cuello rib niña crema 0/3M a 18/24M", "Clothing", None),
        ("Pijama algodón Celeste Gamusa Oso de Bebé New born 0/3 M a 12/24M", "Clothing", None),
        ("Sandalia Cangrejera con Diseño de Oso Beige talla 18 a 21", "Footwear", None),
        ("Corta Viento de niña Con Gorro y Cierre Rojo Talla 3A a 12A", "Clothing", None),
        ("Polar sin mangas niño gris 3/4A a 12/13A", "Clothing", None),
        ("Sobrecamisa mezclilla jeans niño celeste 3/4A a 12/13A", "Clothing", None),
        ("Pantalon buzo niña purpura 3-4A a 12-13A", "Clothing", None),
        ("Poleron paris rayas niña rosa viejo 3-4A a 12-13A", "Clothing", None),
        ("Botella acero inoxidable 354ml Disney Lilo &amp; Stitch niña azul", "Accessories", None),
        ("Zapatilla Filadelfia niño burdeo 28-38", "Footwear", None),
        ("Pack 3 grises Calcetines", "Accessories", None),
        ("Gorro lana unisex bicolor/café 0/3M a 12/24M", "Accessories", None),
        # Material only in the description:
        ("Pijama algodón Negro Camisero de Niño talla 2 a 12 años", "Accessories", DESCRIPCION_EJEMPLO),
        ("Pijama Negro Camisero de Niño talla 2 a 12 años", "Accessories", DESCRIPCION_EJEMPLO),
        # "jeans" and "tejido" in a description must NOT become Denim / Knit:
        (
            "Zapatilla sport niña verde 28-38",
            "Footwear",
            "Zapatilla liviana con capellada de tejido mesh y suela de EVA, "
            "combina perfecto con tus jeans favoritos.",
        ),
        # Composition wins over name and description:
        (
            "Pijama Negro Camisero de Niño talla 2 a 12 años",
            "Accessories",
            DESCRIPCION_EJEMPLO,
            {"Composición": "100% Algodón."},
        ),
        (
            "Jeans skinny niño denim 3-4A a 12-13A",
            "Clothing",
            None,
            {"Composición": "98% Algodón, 2% Elastano"},
        ),
        (
            "Zapatilla sport niña verde 28-38",
            "Footwear",
            None,
            {"Composición": "Capellada: sintético. Suela: caucho"},
        ),
        ("Producto desconocido", None, None),
    ]

    print("\n" + "=" * 160)
    print("CLASSIFICATION TEST")
    print("=" * 160)

    for ejemplo in ejemplos:

        nombre, categoria, descripcion, *resto = ejemplo
        especificaciones = resto[0] if resto else None

        c = clasificar_producto(nombre, categoria, descripcion, especificaciones)

        print(
            f"{html.unescape(nombre)[:52]:<52} | "
            f"{c['product_type']:<14} | "
            f"{c['gender']:<13} | "
            f"{c['age_segment']:<14} | "
            f"{str(c['size_range']):<16} | "
            f"{c['license_character']:<14} | "
            f"{c['material']:<18} | "
            f"{c['material_source']:<18} | "
            f"{c['color']}"
        )

    print("=" * 160 + "\n")

    # ---------- Description sections test (real Colloky HTML) ----------
    html_ejemplo = (
        '<div style="display: contents;"><p>La polera palm con filtro UV es ideal '
        "para que tu niño disfrute de sus días en la playa. Con un diseño atractivo y "
        "funcional, cuenta con un cierre delantero que protege el cuello, asegurando "
        "comodidad y seguridad. Su confección en lycra permite libertad de movimiento, "
        "perfecta para actividades al aire libre, mientras que su protección UV cuida "
        "la piel de los más pequeños de los dañinos rayos solares.</p>"
        "<h2>Consejos de cuidado</h2><ul><li>Lavar a mano con agua fría.</li>"
        "<li>Secar a la sombra.</li><li>Evitar el uso de blanqueadores y planchas.</li></ul>"
        "<h2>Cuándo usarlo</h2><p>Perfecta para días de playa y actividades al aire "
        "libre, esta polera es esencial para proteger a los niños del sol.</p>"
        "<h2>Prendas que combinan</h2><ul><li>Shorts de baño.</li>"
        "<li>Gorros de sol.</li><li>Sandalias cómodas.</li></ul></div>"
    )

    secciones = separar_secciones_descripcion(html_ejemplo)

    print("DESCRIPTION SECTIONS TEST")
    print("=" * 160)

    for clave in SECCIONES_DESCRIPCION:
        print(f"{clave:<18} | {(secciones[clave] or 'None')[:135]}")

    texto_material = " ".join(
        t for t in (secciones["description_main"], secciones["care_tips"]) if t
    )
    c = clasificar_producto(
        "Polera palm filtro uv niño rojo 3-4A a 12-13A", "Clothing", texto_material
    )
    print(f"{'material':<18} | {c['material']} (source: {c['material_source']})")
    print("=" * 160 + "\n")


# =========================================================
# PRICES AND DISCOUNT
# =========================================================

def limpiar_precio(precio):

    if precio is None:
        return 0

    if isinstance(precio, (int, float)):
        return int(precio)

    numeros = re.sub(r"[^0-9]", "", str(precio))

    return int(numeros) if numeros else 0


def calcular_discount(original_price, final_price):
    """(original - final) / original, 4 decimals, never negative."""

    original = limpiar_precio(original_price)
    final = limpiar_precio(final_price)

    if original <= 0 or final <= 0 or final >= original:
        return Decimal("0.0000")

    descuento = Decimal(original - final) / Decimal(original)

    return descuento.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def calcular_ratio(parte, total):

    if not total:
        return None

    return (Decimal(parte) / Decimal(total)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )


# =========================================================
# URL NORMALIZATION
# =========================================================

def normalizar_url_producto(url):

    if not url:
        return ""

    parsed = urlparse(urljoin(URL_TIENDA, url))
    path = parsed.path.rstrip("/")

    if path.endswith("/p"):
        path = path[:-2].rstrip("/")

    return urlunparse(
        (parsed.scheme.lower(), parsed.netloc.lower(), path.lower(), "", "", "")
    )


def slug_desde_url(product_url):
    """https://www.colloky.cl/pijama-negro-xyz -> pijama-negro-xyz"""

    return urlparse(product_url).path.strip("/")


# =========================================================
# JSON-LD (structured data embedded in the page)
# =========================================================

def iterar_productos_json_ld(data):

    if isinstance(data, list):
        objetos = data
    elif isinstance(data, dict):
        if data.get("@type") == "ItemList":
            objetos = data.get("itemListElement", [])
        elif "@graph" in data:
            objetos = data.get("@graph", [])
        else:
            objetos = [data]
    else:
        return

    for elemento in objetos:

        if not isinstance(elemento, dict):
            continue

        producto = elemento.get("item", elemento)

        if not isinstance(producto, dict):
            continue

        tipos = producto.get("@type", [])

        if isinstance(tipos, str):
            tipos = [tipos]

        if "Product" in tipos:
            yield producto


def obtener_lista_ofertas(producto):

    offers = producto.get("offers") or {}

    if isinstance(offers, list):
        return offers, {}

    if not isinstance(offers, dict):
        return [], {}

    lista = offers.get("offers", [])

    if isinstance(lista, dict):
        lista = [lista]

    if not isinstance(lista, list):
        lista = []

    if not lista and offers.get("price") is not None:
        lista = [offers]

    return lista, offers


def extraer_productos_json_ld(html_pagina):

    from bs4 import BeautifulSoup

    productos_json = {}
    soup = BeautifulSoup(html_pagina, "html.parser")

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):

        raw_json = script.string or script.get_text()

        if not raw_json:
            continue

        try:
            data = json.loads(raw_json.strip())
        except (json.JSONDecodeError, TypeError):
            continue

        for producto in iterar_productos_json_ld(data):

            product_key = normalizar_url_producto(
                producto.get("@id") or producto.get("url") or ""
            )

            if not product_key:
                continue

            nombre = html.unescape(str(producto.get("name", ""))).strip()
            lista_ofertas, offers = obtener_lista_ofertas(producto)

            precios_en_stock = []
            precios_agotados = []
            todos_precios = []
            en_stock = 0
            agotadas = 0
            total = 0

            for offer in lista_ofertas:

                if not isinstance(offer, dict):
                    continue

                total += 1
                disponibilidad = str(offer.get("availability", "")).lower()
                precio = limpiar_precio(offer.get("price"))

                if precio:
                    todos_precios.append(precio)

                if any(x in disponibilidad for x in ("outofstock", "soldout", "discontinued")):
                    agotadas += 1
                    if precio:
                        precios_agotados.append(precio)

                elif any(x in disponibilidad for x in ("instock", "limitedavailability", "preorder")):
                    en_stock += 1
                    if precio:
                        precios_en_stock.append(precio)

            final_price = limpiar_precio(offers.get("lowPrice"))

            if not final_price and precios_en_stock:
                final_price = min(precios_en_stock)

            if not final_price and todos_precios:
                final_price = min(todos_precios)

            # Higher prices in the offers (preferably out of stock ones) are
            # the list price before the markdown.
            original_price = 0

            if final_price:
                candidatos = [p for p in precios_agotados if p > final_price]
                if not candidatos:
                    candidatos = [p for p in todos_precios if p > final_price]
                if candidatos:
                    original_price = max(candidatos)

            imagen = producto.get("image", "")

            if isinstance(imagen, list):
                imagen = imagen[0] if imagen else ""

            productos_json[product_key] = {
                "product_name": nombre,
                "sku": str(producto.get("sku", "")).strip(),
                "image_url": str(imagen).strip(),
                # Raw (HTML kept) so the section headings can be detected later.
                "description": str(producto.get("description") or ""),
                "final_price": final_price,
                "original_price": original_price,
                "offers_total": total if lista_ofertas else None,
                "offers_in_stock": en_stock if lista_ofertas else None,
                "offers_out_of_stock": agotadas if lista_ofertas else None,
                "stockout_rate": calcular_ratio(agotadas, en_stock + agotadas),
            }

    return productos_json


# =========================================================
# PRODUCT DESCRIPTIONS (VTEX catalog API, from inside the browser)
# =========================================================
# One batch per page: all the products of the page are requested in
# parallel with fetch(), using the same browser session that is already
# on colloky.cl. Successful results are cached for the whole run.

JS_DESCRIPCIONES = """
const slugs = arguments[0];
const timeoutMs = arguments[1];
const done = arguments[arguments.length - 1];

const fetchOne = async (slug) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(
      '/api/catalog_system/pub/products/search/' + encodeURIComponent(slug) + '/p',
      { signal: controller.signal, credentials: 'same-origin' }
    );
    if (!response.ok) {
      return [slug, { error: 'HTTP ' + response.status }];
    }
    const data = await response.json();
    if (!Array.isArray(data) || data.length === 0) {
      return [slug, { error: 'not found' }];
    }
    const product = data[0];
    const specifications = {};
    for (const name of (product.allSpecifications || [])) {
      const value = product[name];
      specifications[name] = Array.isArray(value)
        ? value.join(', ')
        : (value == null ? '' : String(value));
    }
    return [slug, {
      description: product.description || product.metaTagDescription || '',
      specifications: specifications
    }];
  } catch (error) {
    return [slug, { error: String(error) }];
  } finally {
    clearTimeout(timer);
  }
};

Promise.all(slugs.map(fetchOne))
  .then(results => done(Object.fromEntries(results)))
  .catch(error => done({ __error__: String(error) }));
"""

CACHE_DESCRIPCIONES = {}


def obtener_descripciones(product_urls):
    """
    Returns {product_url: {"description": str, "specifications": dict}}.
    Products whose request failed get an empty dict (they will use the
    JSON-LD description, or only the name, as fallback).
    """
    if not OBTENER_DESCRIPCIONES or not product_urls:
        return {}

    pendientes = {}

    for url in product_urls:
        if url in CACHE_DESCRIPCIONES:
            continue
        slug = slug_desde_url(url)
        if slug:
            pendientes[slug] = url

    if pendientes:

        try:
            respuesta = driver.execute_async_script(
                JS_DESCRIPCIONES, list(pendientes), TIMEOUT_DESCRIPCION_MS
            ) or {}
        except Exception as error:
            print(f"⚠️ Could not fetch descriptions: {error}")
            respuesta = {}

        if respuesta.get("__error__"):
            print(f"⚠️ Description batch error: {respuesta['__error__']}")

        errores = 0

        for slug, url in pendientes.items():

            dato = respuesta.get(slug) or {}

            if not dato or dato.get("error"):
                errores += 1
                continue

            CACHE_DESCRIPCIONES[url] = {
                # Raw HTML: the <h2> headings are needed to split the sections.
                "description": str(dato.get("description") or ""),
                "specifications": dato.get("specifications") or {},
            }

        if errores:
            print(f"⚠️ Descriptions not available: {errores}/{len(pendientes)}")

    return {url: CACHE_DESCRIPCIONES.get(url, {}) for url in product_urls}


# =========================================================
# BUILD ONE ROW
# =========================================================

def construir_registro(
    nombre_raw,
    categoria,
    product_url,
    image_url,
    original_price,
    final_price,
    estructurado,
    page_number,
    page_position,
    category_rank,
    descripcion=None,
    especificaciones=None,
):

    nombre_raw = re.sub(r"\s+", " ", html.unescape(nombre_raw or "")).strip()
    final_price = limpiar_precio(final_price)
    original_price = limpiar_precio(original_price) or final_price
    sku = (estructurado.get("sku") or "").strip()

    # Split the description into its sections.
    secciones = separar_secciones_descripcion(descripcion, especificaciones)

    # Material from the description: only the product's own text.
    # "Prendas que combinan" and "Cuándo usarlo" are left out on purpose.
    texto_para_material = " ".join(
        texto
        for texto in (secciones["description_main"], secciones["care_tips"])
        if texto
    )

    registro = {
        "date_scraped": DATE_SCRAPED,
        "brand": MARCA,
        "category": categoria,
        "page_number": page_number,
        "page_position": page_position,
        "category_rank": category_rank,
        "sku": sku or None,
        "sku_number": int(sku) if sku.isdigit() else None,
        "product_url": product_url,
        "image_url": image_url or estructurado.get("image_url") or None,
        "product_name": limpiar_nombre_producto(nombre_raw),
        "product_name_raw": nombre_raw,
        "product_description": secciones["product_description"],
        "description_main": secciones["description_main"],
        "care_tips": secciones["care_tips"],
        "when_to_use": secciones["when_to_use"],
        "pairs_with": secciones["pairs_with"],
        "original_price": original_price,
        "final_price": final_price,
        "discount": calcular_discount(original_price, final_price),
        "offers_total": estructurado.get("offers_total"),
        "offers_in_stock": estructurado.get("offers_in_stock"),
        "offers_out_of_stock": estructurado.get("offers_out_of_stock"),
        "stockout_rate": estructurado.get("stockout_rate"),
    }

    registro.update(
        clasificar_producto(nombre_raw, categoria, texto_para_material, especificaciones)
    )

    return registro


def imprimir_registro(registro):

    stock = (
        f"{registro['offers_in_stock']}/{registro['offers_total']}"
        if registro["offers_total"] is not None
        else "N/A"
    )

    print(
        f"#{registro['category_rank']:<4} | "
        f"{registro['product_name'][:42]:<42} | "
        f"{registro['product_type']:<12} | "
        f"{registro['gender']:<7} | "
        f"{registro['age_segment']:<14} | "
        f"{registro['material'][:16]:<16} | "
        f"SKU {registro['sku'] or 'N/A':<6} | "
        f"${registro['original_price']} -> ${registro['final_price']} "
        f"({registro['discount']:.0%}) | "
        f"Stock {stock}"
    )


# =========================================================
# LOG FILE (everything printed also goes to logs/...log)
# =========================================================

class _Duplicador:
    """Writes the same text to several streams (screen + log file)."""

    def __init__(self, *flujos):
        self.flujos = [flujo for flujo in flujos if flujo is not None]

    def write(self, texto):
        for flujo in self.flujos:
            try:
                flujo.write(texto)
            except Exception:
                pass
        return len(texto)

    def flush(self):
        for flujo in self.flujos:
            try:
                flujo.flush()
            except Exception:
                pass

    def isatty(self):
        return False


def activar_log():
    """
    Sends every print() to the screen AND to a UTF-8 log file.
    Returns (log_path, log_file, original_stdout) to restore it at the end.
    """
    # Emojis in the output must not crash a Windows console (cp1252).
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    os.makedirs(CARPETA_LOGS, exist_ok=True)

    ruta = os.path.join(
        CARPETA_LOGS, f"colloky_scraper_{DATE_SCRAPED:%Y%m%d_%H%M}.log"
    )

    archivo = open(ruta, "a", encoding="utf-8", buffering=1)
    stdout_original = sys.stdout
    sys.stdout = _Duplicador(stdout_original, archivo)

    return ruta, archivo, stdout_original


# =========================================================
# KEEP THE COMPUTER AWAKE WHILE SCRAPING
# =========================================================

class EvitarSuspension:
    """
    Context manager: asks the operating system not to go to sleep
    while the scraper runs, and releases it at the end (also on errors).
      Windows -> SetThreadExecutionState
      macOS   -> caffeinate (ends by itself when Python ends)
      Linux   -> systemd-inhibit (ends by itself when Python ends)
    """

    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001

    def __enter__(self):

        self.proceso = None
        self.activo = False

        if not EVITAR_SUSPENSION:
            return self

        sistema = platform.system()
        pid = str(os.getpid())

        try:
            if sistema == "Windows":
                import ctypes
                resultado = ctypes.windll.kernel32.SetThreadExecutionState(
                    self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED
                )
                self.activo = bool(resultado)

            elif sistema == "Darwin":
                self.proceso = subprocess.Popen(["caffeinate", "-i", "-w", pid])
                self.activo = True

            elif sistema == "Linux":
                self.proceso = subprocess.Popen(
                    [
                        "systemd-inhibit", "--what=sleep:idle",
                        "--who=Colloky scraper", "--why=Scraping in progress",
                        "--mode=block", "tail", f"--pid={pid}", "-f", "/dev/null",
                    ]
                )
                self.activo = True

        except Exception as error:
            print(f"⚠️ Could not prevent sleep mode: {error}")

        if self.activo:
            print("☕ Sleep prevention ON: the computer will not suspend while scraping.")
        else:
            print("⚠️ Sleep prevention not available: keep the computer awake manually.")

        return self

    def __exit__(self, *args):

        try:
            if platform.system() == "Windows" and self.activo:
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)
            if self.proceso is not None:
                self.proceso.terminate()
        except Exception:
            pass

        if self.activo:
            print("☕ Sleep prevention OFF.")

        return False


# =========================================================
# CHROME
# =========================================================

def iniciar_chrome():

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    opciones = Options()
    opciones.add_argument("--disable-gpu")
    opciones.add_argument("--no-sandbox")
    opciones.add_argument("--disable-dev-shm-usage")

    # Keep Chrome working at full speed even when it is not the active window.
    opciones.add_argument("--disable-background-timer-throttling")
    opciones.add_argument("--disable-renderer-backgrounding")
    opciones.add_argument("--disable-backgrounding-occluded-windows")

    if EJECUTAR_SIN_VENTANA:
        opciones.add_argument("--headless=new")
        opciones.add_argument("--window-size=1920,1080")
    else:
        opciones.add_argument("--start-maximized")

    navegador = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opciones,
    )

    # A window-less Chrome announces itself as "HeadlessChrome"; some stores
    # treat that differently. Use the normal Chrome user agent instead.
    if EJECUTAR_SIN_VENTANA:
        try:
            agente = navegador.execute_script("return navigator.userAgent")
            navegador.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {"userAgent": agente.replace("HeadlessChrome", "Chrome")},
            )
        except Exception as error:
            print(f"⚠️ Could not adjust the user agent: {error}")

    navegador.set_page_load_timeout(60)

    # Time allowed for the batch of description requests of one page.
    navegador.set_script_timeout(max(60, TIMEOUT_DESCRIPCION_MS // 1000 + 30))

    return navegador


def reiniciar_chrome():

    global driver
    global wait

    from selenium.webdriver.support.ui import WebDriverWait

    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass

    print("🔄 Restarting Chrome...")

    driver = iniciar_chrome()
    wait = WebDriverWait(driver, 15)


# =========================================================
# EXTRACT PRODUCTS FROM ONE CATEGORY PAGE
# =========================================================

def obtener_texto(elemento, selector):

    from selenium.webdriver.common.by import By

    try:
        return elemento.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return ""


def obtener_atributo(elemento, selector, atributo):

    from selenium.webdriver.common.by import By

    try:
        valor = elemento.find_element(By.CSS_SELECTOR, selector).get_attribute(atributo)
        return valor.strip() if valor else ""
    except Exception:
        return ""


def extraer_productos(url_pagina, categoria, pagina, rank_inicial):
    """
    Returns (rows, number_of_cards_on_page).
    rank_inicial is the number of products already seen in this category,
    used to compute category_rank (position in the whole category listing).

    Steps:
      1. read every product card (and the JSON-LD) of the page;
      2. fetch the descriptions of all those products in one batch;
      3. build and classify the rows.
    """
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By

    clase_tarjeta = "vtex-product-summary-2-x-container"

    driver.get(url_pagina)

    # An empty VTEX page is the genuine end of the catalogue, not an error.
    try:
        wait.until(
            lambda navegador: (
                len(navegador.find_elements(By.CLASS_NAME, clase_tarjeta)) > 0
                or re.search(
                    r"(no se encontraron|no encontramos|sin resultados|"
                    r"no products|0 resultados|nenhum resultado)",
                    navegador.find_element(By.TAG_NAME, "body").text.lower(),
                )
            )
        )
    except TimeoutException:
        pass

    productos_json = extraer_productos_json_ld(driver.page_source)
    tarjetas = driver.find_elements(By.CLASS_NAME, clase_tarjeta)

    if not tarjetas and not productos_json:
        print("🛑 Empty category page detected. Stopping pagination.")
        return [], 0

    print(f"Product cards: {len(tarjetas)} | Structured products: {len(productos_json)}")

    # ---------- 1. Raw data of every product on the page ----------
    candidatos = []

    if not tarjetas:

        # Fallback: only JSON-LD available (no visual cards).
        for posicion, (product_key, estructurado) in enumerate(productos_json.items(), start=1):

            if not estructurado.get("product_name"):
                continue

            candidatos.append(
                {
                    "nombre_raw": estructurado["product_name"],
                    "product_url": product_key,
                    "image_url": estructurado.get("image_url"),
                    "original_price": estructurado.get("original_price"),
                    "final_price": estructurado.get("final_price"),
                    "estructurado": estructurado,
                    "page_position": posicion,
                }
            )

    else:

        urls_pagina = set()

        for posicion, tarjeta in enumerate(tarjetas, start=1):

            try:
                product_key = normalizar_url_producto(obtener_atributo(tarjeta, "a", "href"))

                if not product_key or product_key in urls_pagina:
                    continue

                urls_pagina.add(product_key)

                estructurado = productos_json.get(product_key, {})

                nombre = (
                    estructurado.get("product_name")
                    or obtener_texto(tarjeta, "span.vtex-product-summary-2-x-productBrand")
                )

                final_price_dom = limpiar_precio(
                    obtener_texto(tarjeta, "span.vtex-product-price-1-x-sellingPrice")
                )

                final_price = final_price_dom or estructurado.get("final_price", 0)
                original_price = estructurado.get("original_price", 0)

                if not original_price:
                    for selector in (
                        "span.vtex-product-price-1-x-listPrice",
                        "span.vtex-product-price-1-x-listPriceValue",
                        "[class*='listPrice']",
                    ):
                        candidato = limpiar_precio(obtener_texto(tarjeta, selector))
                        if candidato and candidato > final_price:
                            original_price = candidato
                            break

                candidatos.append(
                    {
                        "nombre_raw": nombre,
                        "product_url": product_key,
                        "image_url": obtener_atributo(tarjeta, "img", "src"),
                        "original_price": original_price,
                        "final_price": final_price,
                        "estructurado": estructurado,
                        "page_position": posicion,
                    }
                )

            except Exception as error:
                print(f"⚠️ Product extraction error: {error}")

    # ---------- 2. Descriptions, one batch per page ----------
    descripciones = obtener_descripciones([c["product_url"] for c in candidatos])

    # ---------- 3. Build and classify the rows ----------
    registros = []
    con_descripcion = 0

    for candidato in candidatos:

        try:
            extra = descripciones.get(candidato["product_url"], {})

            descripcion = (
                extra.get("description")
                or candidato["estructurado"].get("description")
                or ""
            )

            if descripcion:
                con_descripcion += 1

            registro = construir_registro(
                nombre_raw=candidato["nombre_raw"],
                categoria=categoria,
                product_url=candidato["product_url"],
                image_url=candidato["image_url"],
                original_price=candidato["original_price"],
                final_price=candidato["final_price"],
                estructurado=candidato["estructurado"],
                page_number=pagina,
                page_position=candidato["page_position"],
                category_rank=rank_inicial + candidato["page_position"],
                descripcion=descripcion,
                especificaciones=extra.get("specifications"),
            )

            registros.append(registro)
            imprimir_registro(registro)

        except Exception as error:
            print(f"⚠️ Row build error: {error}")

    if candidatos:
        print(f"📝 Products with description: {con_descripcion}/{len(candidatos)}")

    return registros, (len(tarjetas) if tarjetas else len(productos_json))


# =========================================================
# SKU DEDUPLICATION (within THIS run)
# =========================================================
# If the same SKU shows up again (another page or category), the
# occurrence with LESS stock is kept:
#   1. fewer sizes in stock (offers_in_stock);
#   2. tie -> higher stockout_rate;
#   3. full tie -> the first occurrence.
# Rows of previous snapshots are never touched.

def clave_stock(registro):
    """Lower value = less stock. Unknown stock counts as 'more stock'."""

    en_stock = registro.get("offers_in_stock")
    tasa = registro.get("stockout_rate")

    return (
        en_stock if en_stock is not None else float("inf"),
        -(float(tasa) if tasa is not None else 0.0),
    )


def resumen_sku(registro):
    """What is remembered of each SKU already saved in this run."""

    return {
        "product_url": registro.get("product_url"),
        "offers_in_stock": registro.get("offers_in_stock"),
        "stockout_rate": registro.get("stockout_rate"),
    }


def deduplicar_productos_por_sku(productos, skus_procesados):
    """
    skus_procesados: dict {sku: resumen_sku(...)} shared by the whole run.

    Returns (rows_to_save, urls_to_delete, duplicates):
      rows_to_save   -> rows to upsert;
      urls_to_delete -> URLs of THIS snapshot replaced by an occurrence
                        with less stock that has a different URL.
    """
    a_guardar = []
    urls_a_borrar = []
    duplicados = 0

    for producto in productos:

        sku = (producto.get("sku") or "").strip()

        if not sku:
            a_guardar.append(producto)
            continue

        clave = sku.casefold()
        previo = skus_procesados.get(clave)

        if previo is None:
            skus_procesados[clave] = resumen_sku(producto)
            a_guardar.append(producto)
            continue

        duplicados += 1

        if clave_stock(producto) < clave_stock(previo):

            print(
                f"♻️ Duplicate SKU {sku}: keeping the NEW occurrence "
                f"(less stock: {previo['offers_in_stock']} -> "
                f"{producto.get('offers_in_stock')} sizes in stock)."
            )

            url_previa = previo["product_url"]

            if any(p.get("product_url") == url_previa for p in a_guardar):
                # The previous one is in this same batch: just drop it.
                a_guardar = [p for p in a_guardar if p.get("product_url") != url_previa]
            elif url_previa != producto.get("product_url"):
                # Already saved with another URL: replace that row of this snapshot.
                urls_a_borrar.append(url_previa)

            # Same URL already saved -> the upsert simply updates that row.
            skus_procesados[clave] = resumen_sku(producto)
            a_guardar.append(producto)

        else:
            print(
                f"♻️ Duplicate SKU {sku}: keeping the FIRST occurrence "
                f"(it has less or equal stock)."
            )

    return a_guardar, urls_a_borrar, duplicados


# =========================================================
# DATABASE
# =========================================================

def comprobar_postgresql():

    from sqlalchemy import text

    with engine.connect() as conexion:
        conexion.execute(text("SELECT 1"))

    print("✅ PostgreSQL connection successful")


def crear_tabla():

    from sqlalchemy import text

    ddl = DDL_TABLA

    if os.path.exists(ARCHIVO_SQL):
        with open(ARCHIVO_SQL, encoding="utf-8") as archivo:
            ddl = archivo.read()

    # Remove "--" comments before splitting into statements.
    ddl = re.sub(r"--[^\n]*", "", ddl)

    with engine.begin() as conexion:
        for sentencia in ddl.split(";"):
            if sentencia.strip():
                conexion.execute(text(sentencia))

    print(f"✅ Table {NOMBRE_TABLA} and columns checked")


def guardar_productos(productos, urls_a_borrar=None):
    """
    Upsert keyed on (product_url, date_scraped):
    - a new run (new DATE_SCRAPED) always inserts new rows;
    - re-running the same snapshot only updates that snapshot's rows;
    - rows from previous runs are never touched.
    urls_a_borrar: rows of THIS snapshot replaced by a duplicate SKU with
    less stock (only when it had a different URL).
    """
    from sqlalchemy import text

    columnas = ", ".join(COLUMNAS_TABLA)
    valores = ", ".join(f":{columna}" for columna in COLUMNAS_TABLA)
    actualizaciones = ", ".join(
        f"{columna} = EXCLUDED.{columna}"
        for columna in COLUMNAS_TABLA
        if columna not in CLAVE_TABLA
    )

    consulta = text(
        f"""
        INSERT INTO {NOMBRE_TABLA} ({columnas})
        VALUES ({valores})
        ON CONFLICT (product_url, date_scraped)
        DO UPDATE SET {actualizaciones}
        RETURNING (xmax = 0) AS insertado
        """
    )

    nuevos = 0
    actualizados = 0

    with engine.begin() as conexion:

        for url in urls_a_borrar or []:
            conexion.execute(
                text(
                    f"DELETE FROM {NOMBRE_TABLA} "
                    f"WHERE date_scraped = :fecha AND product_url = :url"
                ),
                {"fecha": DATE_SCRAPED, "url": url},
            )
            print(f"🔁 Replaced (duplicate SKU with less stock): {url}")

        for producto in productos:
            fila = {columna: producto.get(columna) for columna in COLUMNAS_TABLA}
            insertado = conexion.execute(consulta, fila).scalar()
            if insertado:
                nuevos += 1
            else:
                actualizados += 1

    print(f"✅ New: {nuevos} | Updated: {actualizados}")

    return nuevos, actualizados


def cargar_estado_snapshot():
    """
    Used only when resuming (REANUDAR_SNAPSHOT). Reads what this snapshot
    already has in the table so the run can continue where it stopped.

    Returns:
      skus      -> {sku: resumen_sku(...)}  (for the duplicate rule)
      urls      -> {category: set(product_url)}
      paginas   -> {(category, page_number): category rank after that page}
    """
    from sqlalchemy import text

    with engine.connect() as conexion:

        filas = conexion.execute(
            text(
                f"""
                SELECT sku, product_url, category, offers_in_stock, stockout_rate
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).all()

        paginas_db = conexion.execute(
            text(
                f"""
                SELECT
                    category,
                    page_number,
                    MIN(category_rank - page_position) + MAX(page_position) AS rank_final
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha
                GROUP BY category, page_number
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).all()

    skus = {}
    urls = {}

    for sku, url, categoria, en_stock, tasa in filas:
        urls.setdefault(categoria, set()).add(url)
        if sku:
            skus[sku.strip().casefold()] = {
                "product_url": url,
                "offers_in_stock": en_stock,
                "stockout_rate": tasa,
            }

    paginas = {
        (categoria, pagina): int(rank_final or 0)
        for categoria, pagina, rank_final in paginas_db
    }

    return skus, urls, paginas


def resumen_calidad():
    """Quick data-quality check of this run, printed at the end."""

    from sqlalchemy import text

    with engine.connect() as conexion:

        filas = conexion.execute(
            text(
                f"""
                SELECT product_type, COUNT(*) AS total
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha
                GROUP BY product_type
                ORDER BY total DESC
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).all()

        fuentes_material = conexion.execute(
            text(
                f"""
                SELECT material_source, COUNT(*) AS total
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha
                GROUP BY material_source
                ORDER BY total DESC
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).all()

        cobertura = conexion.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE COALESCE(btrim(product_description), '') <> '') AS con_descripcion,
                    COUNT(*) FILTER (WHERE material <> 'Not Specified') AS con_material,
                    COUNT(description_main) AS con_description_main,
                    COUNT(care_tips)        AS con_care_tips,
                    COUNT(when_to_use)      AS con_when_to_use,
                    COUNT(pairs_with)       AS con_pairs_with
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).one()

        otros = conexion.execute(
            text(
                f"""
                SELECT product_name_raw
                FROM {NOMBRE_TABLA}
                WHERE date_scraped = :fecha AND product_type = 'Other'
                LIMIT 20
                """
            ),
            {"fecha": DATE_SCRAPED},
        ).scalars().all()

    print("\n📊 Products per type in this snapshot:")

    for product_type, total in filas:
        print(f"   {product_type:<20} {total}")

    (
        total, con_descripcion, con_material,
        con_main, con_care, con_when, con_pairs,
    ) = cobertura

    if total:
        print(
            f"\n📝 With description: {con_descripcion}/{total} "
            f"({con_descripcion / total:.1%})"
        )
        print(
            f"🧵 With material:    {con_material}/{total} "
            f"({con_material / total:.1%})"
        )
        print("\n📑 Description sections found:")
        for etiqueta, valor in (
            ("description_main", con_main),
            ("care_tips", con_care),
            ("when_to_use", con_when),
            ("pairs_with", con_pairs),
        ):
            print(f"   {etiqueta:<18} {valor}/{total} ({valor / total:.1%})")

    print("\n🧵 Material source:")

    for fuente, total_fuente in fuentes_material:
        print(f"   {fuente:<30} {total_fuente}")

    if otros:
        print("\n🔎 Names classified as 'Other' (review these):")
        for nombre in otros:
            print(f"   - {nombre}")


# =========================================================
# MAIN PROGRAM
# =========================================================

def main():
    """
    Entry point: log file + sleep prevention around the whole run.
    Works the same from Jupyter or from a terminal / .bat file.
    """
    ruta_log, archivo_log, stdout_original = activar_log()

    try:
        print(f"📄 Log file: {os.path.abspath(ruta_log)}")
        print(f"🕒 Snapshot: {DATE_SCRAPED:%Y-%m-%d %H:%M:%S}")
        print(
            "   (if the run is interrupted, resume it with "
            f'REANUDAR_SNAPSHOT = "{DATE_SCRAPED:%Y-%m-%d %H:%M:%S}")'
        )

        with EvitarSuspension():
            ejecutar_scraper()

    finally:
        sys.stdout = stdout_original
        try:
            archivo_log.close()
        except Exception:
            pass


def ejecutar_scraper():

    global engine
    global driver
    global wait

    from selenium.webdriver.support.ui import WebDriverWait

    engine = crear_engine()

    try:
        comprobar_postgresql()
        crear_tabla()

        if EJECUTAR_TESTS:
            probar_clasificacion()

        # ---------- Resume an interrupted snapshot ----------
        skus_procesados = {}
        urls_guardadas = {}
        paginas_guardadas = {}

        if REANUDAR_SNAPSHOT:
            skus_procesados, urls_guardadas, paginas_guardadas = cargar_estado_snapshot()
            if paginas_guardadas:
                print(
                    f"\n⏯️ Resuming snapshot {DATE_SCRAPED}: "
                    f"{len(paginas_guardadas)} pages and {len(skus_procesados)} SKUs "
                    f"already saved will be skipped."
                )
            else:
                print(
                    f"\n⚠️ REANUDAR_SNAPSHOT = {REANUDAR_SNAPSHOT} has no rows yet: "
                    f"starting it from the beginning."
                )

        driver = iniciar_chrome()
        wait = WebDriverWait(driver, 15)

        total_nuevos = 0
        total_actualizados = 0
        total_duplicados = 0
        paginas_fallidas = []

        for nombre_categoria, base_url in CATEGORIAS:

            print("\n" + "#" * 70)
            print(f"🛍️ CATEGORY: {nombre_categoria}")
            print(f"URL: {base_url}")
            print("#" * 70)

            urls_vistas_categoria = set(urls_guardadas.get(nombre_categoria, set()))
            rank_categoria = 0
            finalizar_categoria = False

            for pagina in range(PAGINA_INICIAL, PAGINA_FINAL + 1):

                # Page already saved in the snapshot being resumed.
                if (nombre_categoria, pagina) in paginas_guardadas:
                    rank_categoria = paginas_guardadas[(nombre_categoria, pagina)]
                    print(f"⏭️ {nombre_categoria} page {pagina} already saved. Skipped.")
                    continue

                print("\n" + "=" * 60)
                print(f"Processing {nombre_categoria} - page {pagina}")
                print("=" * 60)

                url_pagina = base_url.format(pagina)
                pagina_completada = False

                for intento in range(1, 4):

                    try:
                        print(f"Attempt {intento} of 3")

                        productos, n_tarjetas = extraer_productos(
                            url_pagina, nombre_categoria, pagina, rank_categoria
                        )

                        if not productos:
                            print(f"🛑 No more products in {nombre_categoria}.")
                            finalizar_categoria = True
                            pagina_completada = True
                            break

                        urls_actuales = {p["product_url"] for p in productos}

                        if not (urls_actuales - urls_vistas_categoria):
                            print("🛑 Page only repeats products already seen. Stopping category.")
                            finalizar_categoria = True
                            pagina_completada = True
                            break

                        urls_vistas_categoria.update(urls_actuales)
                        rank_categoria += n_tarjetas

                        productos, urls_a_borrar, duplicados = deduplicar_productos_por_sku(
                            productos, skus_procesados
                        )
                        total_duplicados += duplicados

                        if productos or urls_a_borrar:
                            nuevos, actualizados = guardar_productos(productos, urls_a_borrar)
                            total_nuevos += nuevos
                            total_actualizados += actualizados

                        pagina_completada = True
                        break

                    except Exception as error:
                        print(f"❌ Error on {nombre_categoria} page {pagina}: {error}")
                        if intento < 3:
                            reiniciar_chrome()
                            time.sleep(3)

                if finalizar_categoria:
                    break

                if not pagina_completada:
                    paginas_fallidas.append(f"{nombre_categoria}:page={pagina}")
                    reiniciar_chrome()

                # Polite delay between pages.
                time.sleep(2)

        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETED")
        print("=" * 60)
        print(f"Snapshot: {DATE_SCRAPED}")
        print(f"Table: {NOMBRE_TABLA}")
        print(f"New rows: {total_nuevos}")
        print(f"Updated rows (same snapshot): {total_actualizados}")
        print(f"Duplicate SKUs found in this run (kept the one with less stock): {total_duplicados}")
        print(f"Failed pages: {paginas_fallidas}")

        resumen_calidad()

    except Exception as error:
        print(f"\n❌ General error: {error}")

    finally:
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass

        if engine is not None:
            engine.dispose()

        print("✅ Chrome and PostgreSQL connections closed")


if __name__ == "__main__":
    main()
