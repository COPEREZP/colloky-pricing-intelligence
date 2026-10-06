-- =====================================================================
-- COLLOKY · DATA QUALITY & ANALYSIS · table colloky_prueba_f
-- ---------------------------------------------------------------------
-- SELECT queries only: nothing is created, modified or deleted.
-- Run them AFTER the scraper (colloky_scraper.py) has finished.
--
-- By default they analyze the LATEST snapshot:
--     WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
-- To analyze another one, replace that subquery with a fixed timestamp.
--
-- Note: when the scraper cannot find a value it writes a placeholder
-- ('Not Specified', 'Color Not Specified', 'Other', 'N/A') instead of NULL.
-- That is why NULLs, blanks and placeholders are measured separately and
-- then together ("missing").
-- =====================================================================


-- =====================================================================
-- Q0. SNAPSHOT OVERVIEW
-- Rows per run. A sudden drop between snapshots usually means the run
-- was interrupted or the website changed.
-- =====================================================================
SELECT
    date_scraped,
    COUNT(*)                                         AS total_rows,
    COUNT(*) FILTER (WHERE category = 'Accessories') AS accessories,
    COUNT(*) FILTER (WHERE category = 'Clothing')    AS clothing,
    COUNT(*) FILTER (WHERE category = 'Footwear')    AS footwear,
    MIN(created_at)                                  AS first_insert,
    MAX(created_at)                                  AS last_insert
FROM colloky_prueba_f
GROUP BY date_scraped
ORDER BY date_scraped DESC;


-- =====================================================================
-- Q1. FULL PROFILE, ONE ROW PER COLUMN (latest snapshot)
-- Each row is converted to JSON and unpivoted into column/value pairs,
-- so every column (including future ones) is profiled automatically.
--   nulls        -> real NULL
--   blanks       -> empty or whitespace-only text
--   placeholders -> 'Not Specified', 'Color Not Specified', 'Other', 'N/A'
--   zeros        -> value 0 (in discount or color_count a 0 can be legit)
--   distinct_values -> 1 means the column is constant
--   pct_missing  -> nulls + blanks + placeholders
-- "most_frequent_value" is only meaningful for categorical columns
-- (for 100% unique columns it just shows the smallest value).
-- =====================================================================
WITH latest AS (
    SELECT *
    FROM colloky_prueba_f
    WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
),
pairs AS (
    SELECT kv.key AS column_name, kv.value AS value
    FROM latest l
    CROSS JOIN LATERAL jsonb_each_text(to_jsonb(l)) AS kv
)
SELECT
    column_name,
    COUNT(*)                                                                AS total_rows,
    COUNT(*) FILTER (WHERE value IS NULL)                                   AS nulls,
    COUNT(*) FILTER (WHERE value IS NOT NULL AND btrim(value) = '')         AS blanks,
    COUNT(*) FILTER (WHERE value IN ('Not Specified', 'Color Not Specified', 'Other', 'N/A')) AS placeholders,
    COUNT(*) FILTER (WHERE value ~ '^0+(\.0+)?$')                           AS zeros,
    COUNT(DISTINCT value)                                                   AS distinct_values,
    LEFT(mode() WITHIN GROUP (ORDER BY value), 80)                          AS most_frequent_value,
    ROUND(100.0 * COUNT(*) FILTER (WHERE value IS NULL) / COUNT(*), 2)      AS pct_nulls,
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE value IS NULL
           OR btrim(value) = ''
           OR value IN ('Not Specified', 'Color Not Specified', 'Other', 'N/A')
    ) / COUNT(*), 2)                                                        AS pct_missing
FROM pairs
GROUP BY column_name
ORDER BY pct_missing DESC, column_name;


-- =====================================================================
-- Q2. COLUMNS THAT ARE NOT BRINGING USEFUL DATA (latest snapshot)
--   100% missing      -> dead column
--   1 distinct value  -> constant column
--   > 50% missing     -> mostly empty column
-- =====================================================================
WITH latest AS (
    SELECT *
    FROM colloky_prueba_f
    WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
),
pairs AS (
    SELECT kv.key AS column_name, kv.value AS value
    FROM latest l
    CROSS JOIN LATERAL jsonb_each_text(to_jsonb(l)) AS kv
),
profile AS (
    SELECT
        column_name,
        COUNT(DISTINCT value) AS distinct_values,
        ROUND(100.0 * COUNT(*) FILTER (
            WHERE value IS NULL
               OR btrim(value) = ''
               OR value IN ('Not Specified', 'Color Not Specified', 'Other', 'N/A')
        ) / COUNT(*), 2) AS pct_missing
    FROM pairs
    GROUP BY column_name
)
SELECT
    column_name,
    distinct_values,
    pct_missing,
    CASE
        WHEN pct_missing = 100   THEN 'DEAD: no data at all'
        WHEN distinct_values <= 1 THEN 'CONSTANT: always the same value'
        ELSE                          'MOSTLY EMPTY (>50%)'
    END AS diagnosis
FROM profile
WHERE pct_missing = 100
   OR distinct_values <= 1
   OR pct_missing > 50
ORDER BY pct_missing DESC;


-- =====================================================================
-- Q3. % MISSING BY COLUMN AND CATEGORY (latest snapshot)
-- Shows whether a gap is general or specific to one category.
-- =====================================================================
WITH latest AS (
    SELECT *
    FROM colloky_prueba_f
    WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
),
pairs AS (
    SELECT
        l.category,
        kv.key AS column_name,
        (kv.value IS NULL
         OR btrim(kv.value) = ''
         OR kv.value IN ('Not Specified', 'Color Not Specified', 'Other', 'N/A')) AS is_missing
    FROM latest l
    CROSS JOIN LATERAL jsonb_each_text(to_jsonb(l)) AS kv
)
SELECT
    column_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Accessories' AND is_missing)
          / NULLIF(COUNT(*) FILTER (WHERE category = 'Accessories'), 0), 2) AS pct_missing_accessories,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Clothing' AND is_missing)
          / NULLIF(COUNT(*) FILTER (WHERE category = 'Clothing'), 0), 2)    AS pct_missing_clothing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Footwear' AND is_missing)
          / NULLIF(COUNT(*) FILTER (WHERE category = 'Footwear'), 0), 2)    AS pct_missing_footwear,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_missing) / COUNT(*), 2)         AS pct_missing_total
FROM pairs
GROUP BY column_name
ORDER BY pct_missing_total DESC, column_name;


-- =====================================================================
-- Q4. % MISSING ACROSS SNAPSHOTS (last 8)
-- A sudden jump from one week to the next usually means the website
-- changed and the scraper stopped finding that field.
-- =====================================================================
WITH last_snapshots AS (
    SELECT DISTINCT date_scraped
    FROM colloky_prueba_f
    ORDER BY date_scraped DESC
    LIMIT 8
),
pairs AS (
    SELECT
        t.date_scraped,
        kv.key AS column_name,
        (kv.value IS NULL
         OR btrim(kv.value) = ''
         OR kv.value IN ('Not Specified', 'Color Not Specified', 'Other', 'N/A')) AS is_missing
    FROM colloky_prueba_f t
    JOIN last_snapshots s USING (date_scraped)
    CROSS JOIN LATERAL jsonb_each_text(to_jsonb(t)) AS kv
)
SELECT
    column_name,
    date_scraped,
    COUNT(*)                                                        AS total_rows,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_missing) / COUNT(*), 2) AS pct_missing
FROM pairs
GROUP BY column_name, date_scraped
ORDER BY column_name, date_scraped DESC;


-- =====================================================================
-- Q5. CONSISTENCY CHECKS (latest snapshot)
-- Values that are present but wrong or inconsistent.
-- Ideally most of these return 0.
-- =====================================================================
WITH l AS (
    SELECT *
    FROM colloky_prueba_f
    WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
),
checks AS (
    SELECT 'Final price null or <= 0' AS check_name,
           COUNT(*) FILTER (WHERE final_price IS NULL OR final_price <= 0) AS affected_rows FROM l
    UNION ALL
    SELECT 'Original price null or <= 0',
           COUNT(*) FILTER (WHERE original_price IS NULL OR original_price <= 0) FROM l
    UNION ALL
    SELECT 'Final price HIGHER than original price',
           COUNT(*) FILTER (WHERE final_price > original_price) FROM l
    UNION ALL
    SELECT 'Discount out of range (<0 or >=1)',
           COUNT(*) FILTER (WHERE discount < 0 OR discount >= 1) FROM l
    UNION ALL
    SELECT 'Discount does not match prices',
           COUNT(*) FILTER (
               WHERE original_price > 0
                 AND final_price < original_price
                 AND ABS(discount - ROUND((original_price - final_price)::numeric / original_price, 4)) > 0.0001
           ) FROM l
    UNION ALL
    SELECT 'No discount (discount = 0)',
           COUNT(*) FILTER (WHERE discount = 0) FROM l
    UNION ALL
    SELECT 'Stock: in stock + out of stock <> total offers',
           COUNT(*) FILTER (
               WHERE offers_total IS NOT NULL
                 AND COALESCE(offers_in_stock, 0) + COALESCE(offers_out_of_stock, 0) <> offers_total
           ) FROM l
    UNION ALL
    SELECT 'Stock: no offer data (offers_total null)',
           COUNT(*) FILTER (WHERE offers_total IS NULL) FROM l
    UNION ALL
    SELECT 'Stock: 0 sizes available',
           COUNT(*) FILTER (WHERE offers_in_stock = 0) FROM l
    UNION ALL
    SELECT 'stockout_rate null while having offers',
           COUNT(*) FILTER (WHERE stockout_rate IS NULL AND COALESCE(offers_total, 0) > 0) FROM l
    UNION ALL
    SELECT 'SKU null or blank',
           COUNT(*) FILTER (WHERE sku IS NULL OR btrim(sku) = '') FROM l
    UNION ALL
    SELECT 'Non-numeric SKU (sku_number null while sku present)',
           COUNT(*) FILTER (WHERE sku IS NOT NULL AND btrim(sku) <> '' AND sku_number IS NULL) FROM l
    UNION ALL
    SELECT 'Image null or not starting with http',
           COUNT(*) FILTER (WHERE image_url IS NULL OR image_url NOT LIKE 'http%') FROM l
    UNION ALL
    SELECT 'URL not from colloky.cl',
           COUNT(*) FILTER (WHERE product_url NOT LIKE 'https://www.colloky.cl/%') FROM l
    UNION ALL
    SELECT 'Page position or rank null',
           COUNT(*) FILTER (WHERE page_number IS NULL OR page_position IS NULL OR category_rank IS NULL) FROM l
    UNION ALL
    SELECT 'color_count = 0 but a color is present',
           COUNT(*) FILTER (WHERE color_count = 0 AND color <> 'Color Not Specified') FROM l
    UNION ALL
    SELECT 'Inconsistent license (is_licensed vs license_brand)',
           COUNT(*) FILTER (
               WHERE (is_licensed AND license_brand = 'Own Brand')
                  OR (NOT is_licensed AND license_brand <> 'Own Brand')
           ) FROM l
    UNION ALL
    SELECT 'product_type = Other (unclassified)',
           COUNT(*) FILTER (WHERE product_type = 'Other') FROM l
    UNION ALL
    SELECT 'No description (product_description null or empty)',
           COUNT(*) FILTER (WHERE COALESCE(btrim(product_description), '') = '') FROM l
    UNION ALL
    SELECT 'Material Not Specified despite having a description',
           COUNT(*) FILTER (
               WHERE material = 'Not Specified'
                 AND COALESCE(btrim(product_description), '') <> ''
           ) FROM l
    UNION ALL
    SELECT 'No composition (material_composition null)',
           COUNT(*) FILTER (WHERE material_composition IS NULL) FROM l
    UNION ALL
    SELECT 'Has composition but material did NOT come from it',
           COUNT(*) FILTER (
               WHERE material_composition IS NOT NULL
                 AND material_source <> 'specification'
           ) FROM l
    UNION ALL
    SELECT 'No "Description" section (description_main null)',
           COUNT(*) FILTER (WHERE description_main IS NULL) FROM l
    UNION ALL
    SELECT 'No "Care tips" section (care_tips null)',
           COUNT(*) FILTER (WHERE care_tips IS NULL) FROM l
    UNION ALL
    SELECT 'No "When to wear it" section (when_to_use null)',
           COUNT(*) FILTER (WHERE when_to_use IS NULL) FROM l
    UNION ALL
    SELECT 'No "Pairs well with" section (pairs_with null)',
           COUNT(*) FILTER (WHERE pairs_with IS NULL) FROM l
    UNION ALL
    SELECT 'Has a description but NO section heading was detected',
           COUNT(*) FILTER (
               WHERE COALESCE(btrim(product_description), '') <> ''
                 AND care_tips IS NULL AND when_to_use IS NULL AND pairs_with IS NULL
           ) FROM l
    UNION ALL
    SELECT 'Inconsistent material vs material_source',
           COUNT(*) FILTER (
               WHERE (material = 'Not Specified') <> (material_source = 'Not Specified')
           ) FROM l
)
SELECT
    check_name,
    affected_rows,
    ROUND(100.0 * affected_rows / NULLIF((SELECT COUNT(*) FROM l), 0), 2) AS pct
FROM checks
ORDER BY affected_rows DESC;


-- =====================================================================
-- Q6. DUPLICATE SKUs WITHIN THE SAME SNAPSHOT
-- (product_url + date_scraped is already unique by index; this checks SKU)
-- =====================================================================
SELECT
    sku,
    COUNT(*)                            AS occurrences,
    string_agg(DISTINCT category, ', ') AS categories,
    string_agg(product_url, ' | ')      AS urls
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND sku IS NOT NULL
GROUP BY sku
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;


-- =====================================================================
-- Q7. WHICH PRODUCT TYPES EXPLAIN THE "Not Specified" VALUES? (latest snapshot)
-- Helps tell expected gaps (e.g. a sock pack with no gender) from
-- classification problems worth fixing.
-- =====================================================================
SELECT
    product_type,
    COUNT(*) AS products,
    ROUND(100.0 * COUNT(*) FILTER (WHERE gender = 'Not Specified')      / COUNT(*), 1) AS pct_no_gender,
    ROUND(100.0 * COUNT(*) FILTER (WHERE age_segment = 'Not Specified') / COUNT(*), 1) AS pct_no_age_segment,
    ROUND(100.0 * COUNT(*) FILTER (WHERE size_range IS NULL)            / COUNT(*), 1) AS pct_no_size_range,
    ROUND(100.0 * COUNT(*) FILTER (WHERE material = 'Not Specified')    / COUNT(*), 1) AS pct_no_material,
    ROUND(100.0 * COUNT(*) FILTER (WHERE color = 'Color Not Specified') / COUNT(*), 1) AS pct_no_color,
    ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(btrim(product_description), '') = '') / COUNT(*), 1) AS pct_no_description
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
GROUP BY product_type
ORDER BY products DESC;


-- =====================================================================
-- Q8. WHERE THE MATERIAL COMES FROM (latest snapshot)
-- Scraper priority:
--   specification -> the "Composición" specification (primary, most precise)
--   name / description / name+description -> fallback, only when there is
--                    no composition or it has no known material
--   Not Specified -> not found in any source
-- =====================================================================
SELECT
    material_source,
    COUNT(*)                                            AS products,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS pct
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
GROUP BY material_source
ORDER BY products DESC;


-- =====================================================================
-- Q9. FREQUENCY OF EACH INDIVIDUAL MATERIAL (latest snapshot)
-- The material column can hold several values ("Cotton, Rib Knit");
-- they are split here so each one is counted.
-- =====================================================================
SELECT
    btrim(m.single_material)                                          AS material,
    COUNT(*)                                                          AS products,
    COUNT(*) FILTER (WHERE t.material_source LIKE '%description%')    AS found_in_description
FROM colloky_prueba_f t
CROSS JOIN LATERAL unnest(string_to_array(t.material, ',')) AS m(single_material)
WHERE t.date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND t.material <> 'Not Specified'
GROUP BY btrim(m.single_material)
ORDER BY products DESC;


-- =====================================================================
-- Q10. MANUAL REVIEW: material detected ONLY from the description
-- Random sample of 50 to spot false positives by eye.
-- (The scraper only looks for material in "Description" and "Care tips";
-- "Pairs well with" is ignored on purpose, since it talks about OTHER products.)
-- =====================================================================
SELECT
    product_name_raw,
    material,
    LEFT(description_main, 300) AS description_start,
    care_tips
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND material_source = 'description'
ORDER BY random()
LIMIT 50;


-- =====================================================================
-- Q11. MANUAL REVIEW: has a description but NO material was detected
-- Random sample of 50: materials that are not in the scraper's list
-- (e.g. "poliéster", "goma") are candidates to add.
-- =====================================================================
SELECT
    product_type,
    product_name_raw,
    LEFT(product_description, 300) AS description_start
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND material = 'Not Specified'
  AND COALESCE(btrim(product_description), '') <> ''
ORDER BY random()
LIMIT 50;


-- =====================================================================
-- Q12. MATERIAL WORDS THE SCRAPER DOES NOT RECOGNIZE YET
-- Among products WITHOUT material but WITH a description, counts how many
-- mention typical composition words missing from the scraper's list.
-- The words are in Spanish on purpose: they are searched inside the
-- store's (Spanish) product descriptions.
-- =====================================================================
WITH no_material AS (
    SELECT lower(product_description) AS description
    FROM colloky_prueba_f
    WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
      AND material = 'Not Specified'
      AND COALESCE(btrim(product_description), '') <> ''
)
SELECT keyword, COUNT(*) AS products
FROM no_material
CROSS JOIN (VALUES
    ('textil'), ('felpa'), ('piqué'), ('pique'), ('modal'), ('bambú'), ('bambu'),
    ('seda'), ('satín'), ('satin'), ('tul'), ('lurex'), ('neopreno'), ('silicona'),
    ('goma'), ('caucho'), ('sintético'), ('sintetico')
) AS v(keyword)
WHERE description LIKE '%' || keyword || '%'
GROUP BY keyword
ORDER BY products DESC;


-- =====================================================================
-- Q13. COMPOSITIONS THE SCRAPER COULD NOT MAP TO A MATERIAL
-- Composition texts published by the store where no known material was
-- recognized. A new word here (e.g. "lurex") is a candidate to add to
-- the scraper's material list.
-- =====================================================================
SELECT
    material_composition,
    COUNT(*)                     AS products,
    MIN(product_name_raw)        AS example_product
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND material_composition IS NOT NULL
  AND material_source <> 'specification'
GROUP BY material_composition
ORDER BY products DESC;


-- =====================================================================
-- Q14. MOST FREQUENT COMPOSITIONS (latest snapshot)
-- Quick view of how the store publishes composition, next to the
-- material the scraper assigned.
-- =====================================================================
SELECT
    material_composition,
    material,
    COUNT(*) AS products
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND material_composition IS NOT NULL
GROUP BY material_composition, material
ORDER BY products DESC
LIMIT 50;


-- =====================================================================
-- Q15. COMPARISON WITH THE PREVIOUS TABLE (colloky_product_snapshots)
-- % of products with a known material: before (name only) vs now.
-- Skip this query if the previous table does not exist in your database.
-- =====================================================================
SELECT
    'colloky_product_snapshots (v2)' AS table_name,
    MAX(date_scraped)                AS snapshot,
    COUNT(*)                         AS total_rows,
    ROUND(100.0 * COUNT(*) FILTER (WHERE material <> 'Not Specified') / COUNT(*), 2) AS pct_with_material
FROM colloky_product_snapshots
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_product_snapshots)
UNION ALL
SELECT
    'colloky_prueba_f (v3)',
    MAX(date_scraped),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE material <> 'Not Specified') / COUNT(*), 2)
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f);


-- =====================================================================
-- Q16. DESCRIPTION SECTION COVERAGE BY CATEGORY
-- % of products that have each section of the product page:
-- "Description", "Care tips", "When to wear it", "Pairs well with".
-- =====================================================================
SELECT
    category,
    COUNT(*)                                             AS products,
    ROUND(100.0 * COUNT(description_main) / COUNT(*), 1) AS pct_description,
    ROUND(100.0 * COUNT(care_tips)        / COUNT(*), 1) AS pct_care_tips,
    ROUND(100.0 * COUNT(when_to_use)      / COUNT(*), 1) AS pct_when_to_use,
    ROUND(100.0 * COUNT(pairs_with)       / COUNT(*), 1) AS pct_pairs_with
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
GROUP BY ROLLUP (category)
ORDER BY category NULLS LAST;


-- =====================================================================
-- Q17. MOST FREQUENT CARE TIPS
-- Splits each tip (they come as sentences ending in a period, e.g.
-- "Lavar a mano con agua fría. Secar a la sombra.") and counts them.
-- Useful to see care patterns by product type.
-- =====================================================================
SELECT
    btrim(c.tip)                              AS care_tip,
    COUNT(*)                                  AS products,
    string_agg(DISTINCT t.product_type, ', ') AS product_types
FROM colloky_prueba_f t
CROSS JOIN LATERAL regexp_split_to_table(t.care_tips, '\.\s+') AS c(tip)
WHERE t.date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND t.care_tips IS NOT NULL
  AND btrim(c.tip) <> ''
GROUP BY btrim(c.tip)
ORDER BY products DESC
LIMIT 30;


-- =====================================================================
-- Q18. PRODUCT ZOOM: FULL PRODUCT SHEET (latest snapshot)
-- Replace SKU '61237' with the product you want to review.
-- =====================================================================
SELECT
    sku,
    product_name_raw,
    category,
    product_type,
    gender,
    age_segment,
    size_range,
    license_character,
    material,
    material_source,
    material_composition,
    color,
    original_price,
    final_price,
    discount,
    offers_in_stock,
    offers_total,
    stockout_rate,
    category_rank,
    description_main,
    care_tips,
    when_to_use,
    pairs_with,
    product_url
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND sku = '61237';


-- =====================================================================
-- Q19. PRODUCT ZOOM: HISTORY ACROSS SNAPSHOTS
-- How price, discount, stock and store visibility evolved.
-- Replace SKU '61237' with the product you want to review.
-- =====================================================================
SELECT
    date_scraped,
    original_price,
    final_price,
    discount,
    offers_in_stock,
    offers_total,
    stockout_rate,
    page_number,
    category_rank,
    final_price - LAG(final_price) OVER (ORDER BY date_scraped)         AS price_change,
    offers_in_stock - LAG(offers_in_stock) OVER (ORDER BY date_scraped) AS available_sizes_change
FROM colloky_prueba_f
WHERE sku = '61237'
ORDER BY date_scraped;


-- =====================================================================
-- Q20. "BEST SELLER" CANDIDATES (PROXY) WITH THEIR PRODUCT SHEET
-- There is no real sales data: as a proxy, size stock-outs are used
-- (high stockout_rate = several sizes sold out) together with store
-- placement (low category_rank = the store promotes it).
-- With 2+ snapshots, the week-over-week drop in available sizes is a
-- better signal (see Q19).
-- =====================================================================
SELECT
    sku,
    product_name_raw,
    category,
    product_type,
    final_price,
    discount,
    offers_in_stock,
    offers_total,
    stockout_rate,
    category_rank,
    material,
    LEFT(description_main, 200) AS description,
    when_to_use,
    pairs_with
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
  AND offers_total >= 3
ORDER BY stockout_rate DESC, category_rank ASC
LIMIT 20;


-- =====================================================================
-- Q21. WHY DO ~74% OF PRODUCTS LACK "CARE TIPS / WHEN TO WEAR IT / PAIRS WELL WITH"?
-- If the 3 sections are missing on EXACTLY the same products, it is not a
-- scraper failure: the store wrote the "full" product sheet only for part
-- of the catalog. This query compares both groups.
-- A higher average sku_number and lower discount in the "With sections"
-- group suggests they are the NEW products (current season).
-- =====================================================================
SELECT
    CASE WHEN care_tips IS NOT NULL OR when_to_use IS NOT NULL OR pairs_with IS NOT NULL
         THEN 'With sections' ELSE 'Without sections' END             AS product_group,
    COUNT(*)                                                         AS products,
    ROUND(AVG(sku_number))                                           AS avg_sku,
    MIN(sku_number)                                                  AS min_sku,
    MAX(sku_number)                                                  AS max_sku,
    ROUND(100.0 * AVG(discount), 1)                                  AS avg_discount_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE discount = 0) / COUNT(*), 1) AS pct_no_discount,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Accessories') / COUNT(*), 1) AS pct_accessories,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Clothing')    / COUNT(*), 1) AS pct_clothing,
    ROUND(100.0 * COUNT(*) FILTER (WHERE category = 'Footwear')    / COUNT(*), 1) AS pct_footwear,
    -- Do the 3 sections always come together?
    COUNT(*) FILTER (WHERE (care_tips IS NULL) <> (when_to_use IS NULL)
                        OR (care_tips IS NULL) <> (pairs_with IS NULL))     AS products_with_incomplete_sections
FROM colloky_prueba_f
WHERE date_scraped = (SELECT MAX(date_scraped) FROM colloky_prueba_f)
GROUP BY 1
ORDER BY 1;
