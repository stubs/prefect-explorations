{{
    config(materialized='external', location='/Users/agonzalez/duck_out.json')
}}

with final AS (
    SELECT
        * EXCLUDE (images),
        UNNEST(images) as image
    FROM {{ source("external_sources", "raw_products")}}

)
SELECT * FROM final
