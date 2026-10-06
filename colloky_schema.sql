-- =====================================================================
-- COLLOKY · TABLE DEFINITION: colloky_prueba_f
-- ---------------------------------------------------------------------
-- The scraper runs this file automatically at start-up (if it finds it in
-- the same folder; otherwise it uses the same definition embedded in the
-- script). You do not need to run it by hand, but you can, to create the
-- table before the first run. Every statement is idempotent: it is safe
-- to run it again.
--
-- Do NOT add SELECT queries to this file: the scraper executes everything
-- in it.
-- =====================================================================

CREATE TABLE IF NOT EXISTS colloky_prueba_f (
    id                  BIGSERIAL PRIMARY KEY,
    date_scraped        TIMESTAMP     NOT NULL,
    brand               VARCHAR(100)  NOT NULL,
    product_url         TEXT          NOT NULL
);

ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS category            VARCHAR(50);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS page_number         INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS page_position       INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS category_rank       INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS sku                 VARCHAR(50);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS sku_number          INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS image_url           TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS product_name        VARCHAR(500);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS product_name_raw    VARCHAR(500);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS product_description TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS description_main    TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS care_tips           TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS when_to_use         TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS pairs_with          TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS product_type        VARCHAR(100);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS gender              VARCHAR(30);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS age_segment         VARCHAR(30);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS age_min_months      INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS size_range          VARCHAR(100);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS is_licensed         BOOLEAN DEFAULT FALSE;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS license_brand       VARCHAR(50);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS license_character   VARCHAR(100);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS material            VARCHAR(200);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS material_source     VARCHAR(50);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS material_composition TEXT;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS color               VARCHAR(250);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS color_type          VARCHAR(50);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS color_count         INTEGER DEFAULT 0;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS original_price      INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS final_price         INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS discount            NUMERIC(6,4);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS offers_total        INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS offers_in_stock     INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS offers_out_of_stock INTEGER;
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS stockout_rate       NUMERIC(6,4);
ALTER TABLE colloky_prueba_f ADD COLUMN IF NOT EXISTS created_at          TIMESTAMP DEFAULT NOW();

-- A product appears only once per snapshot.
CREATE UNIQUE INDEX IF NOT EXISTS ux_colloky_prueba_f_url_date
    ON colloky_prueba_f (product_url, date_scraped);

CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_date     ON colloky_prueba_f (date_scraped);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_sku      ON colloky_prueba_f (sku);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_type     ON colloky_prueba_f (product_type);
CREATE INDEX IF NOT EXISTS ix_colloky_prueba_f_material ON colloky_prueba_f (material);
