-- TIXR: PAGEVIEW HT EVENTS MODEL --

WITH IdentifiesRanked AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY EXTERNAL_ID ORDER BY TIMESTAMP DESC) as rn
    FROM <<SCHEMA_NAME_GOES_HERE>>.IDENTIFIES
),
OrdersRanked AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY EXTERNAL_ID ORDER BY TIMESTAMP DESC) as rn
    FROM <<SCHEMA_NAME_GOES_HERE>>.PURCHASE
),
UUIDRanked AS (
SELECT
    -- EVENT DETAILS
    p.UUID as UUID,
    p.TIMESTAMP as TIMESTAMP,
    p.CONTEXT_PAGE_URL as PAGE_URL,
    p.CONTEXT_PAGE_PATH as PAGE_PATH,
    p.CONTEXT_PAGE_TITLE as PAGE_TITLE,
    p.CONTEXT_PAGE_SEARCH as PAGE_SEARCH,
    p.CONTEXT_PAGE_REFERRER as PAGE_REFERRER,
    p.FBC as FBC,
    
    -- USER DETAILS
    COALESCE(p.EXTERNAL_ID, i.EXTERNAL_ID, o.EXTERNAL_ID) as EXTERNAL_ID,
    TRIM(LOWER(COALESCE(i.EMAIL, p.EMAIL, o.EMAIL, p.CUSTOM_EMAIL))) as EMAIL,
    CASE 
        WHEN COALESCE(p.PHONE_NUMBER, o.PHONE_NUMBER, i.PHONE_NUMBER) IS NOT NULL THEN
            CASE
                WHEN REGEXP_LIKE(COALESCE(p.PHONE_NUMBER, o.PHONE_NUMBER, i.PHONE_NUMBER), '^\\+1[2-9][0-9]{2}[2-9][0-9]{6}$') THEN
                    COALESCE(p.PHONE_NUMBER, o.PHONE_NUMBER, i.PHONE_NUMBER)
                WHEN REGEXP_LIKE(
                    REGEXP_REPLACE(COALESCE(p.PHONE_NUMBER, o.PHONE_NUMBER, i.PHONE_NUMBER), '^((\\+1\\+?)|(\\+1)|(\\+))', ''),
                    '^[2-9][0-9]{2}[2-9][0-9]{6}$'
                ) THEN
                    '+1' || REGEXP_REPLACE(
                        COALESCE(p.PHONE_NUMBER, o.PHONE_NUMBER, i.PHONE_NUMBER),
                        '^((\\+1\\+?)|(\\+1)|(\\+))',
                        ''
                    )
                ELSE NULL
            END
        ELSE NULL
    END AS PHONE_NUMBER,
    TRIM(LOWER(COALESCE(i.FIRST_NAME, o.FIRST_NAME, p.FIRST_NAME))) as FIRST_NAME,
    TRIM(LOWER(LEFT(COALESCE(i.FIRST_NAME, o.FIRST_NAME, p.FIRST_NAME), 1))) as FIRST_INITIAL,
    TRIM(LOWER(COALESCE(i.LAST_NAME, o.LAST_NAME, p.LAST_NAME))) as LAST_NAME,
    TO_CHAR(TO_DATE(COALESCE(p.BIRTHDAY, o.BIRTHDAY), 'DD-MM-YYYY'), 'YYYYMMDD') AS BIRTHDAY,
    TRIM(LOWER(LEFT(COALESCE(p.GENDER, o.GENDER),1))) AS GENDER,
    COALESCE(p.POSTAL_CODE, o.POSTAL_CODE) AS ZIP_CODE,
    COALESCE(p.FBP, i.FBP, o.FBP) as FBP,
    p.CONTEXT_IP as IP_ADDRESS,
    p.CONTEXT_USER_AGENT as USER_AGENT,
    p.CONTEXT_TIMEZONE as USER_TIMEZONE,
    p.CONTEXT_LOCALE as USER_LOCALE,
    LOWER(SPLIT_PART(p.CONTEXT_LOCALE, '-', 1)) as LANGUAGE_TAG,
    CASE 
        WHEN LENGTH(LOWER(SPLIT_PART(p.CONTEXT_LOCALE, '-', 2))) = 2 THEN LOWER(SPLIT_PART(p.CONTEXT_LOCALE, '-', 2))
        ELSE NULL
    END as COUNTRY_CODE,

    -- ADVERTISING IDS
    -- GOOGLE: Extract the gclid from CONTEXT_PAGE_SEARCH and store it in a new column GCLID
        CASE
            WHEN p.CONTEXT_PAGE_SEARCH LIKE '%gclid=%'
            THEN SPLIT_PART(SPLIT_PART(p.CONTEXT_PAGE_SEARCH, 'gclid=', 2), '&', 1)
            ELSE NULL
        END AS GCLID,
        
    -- GOOGLE: Extract the gbraid from CONTEXT_PAGE_URL and store it in a new column GBRAID
        CASE
            WHEN p.CONTEXT_PAGE_URL LIKE '%gbraid=%'
            THEN SPLIT_PART(SPLIT_PART(p.CONTEXT_PAGE_URL, 'gbraid=', 2), '&', 1)
            ELSE NULL
        END AS GBRAID,

    -- GOOGLE: Extract the wbraid from CONTEXT_PAGE_URL and store it in a new column WBRAID
        CASE
            WHEN p.CONTEXT_PAGE_URL LIKE '%wbraid=%'
            THEN SPLIT_PART(SPLIT_PART(p.CONTEXT_PAGE_URL, 'wbraid=', 2), '&', 1)
            ELSE NULL
        END AS WBRAID,

    -- TIKTOK: Extract the ttclid from CONTEXT_PAGE_URL and store it in a new column TTCLID
        CASE
            WHEN p.CONTEXT_PAGE_URL LIKE '%ttclid=%'
            THEN SPLIT_PART(SPLIT_PART(p.CONTEXT_PAGE_URL, 'ttclid=', 2), '&', 1)
            ELSE NULL
        END AS TTCLID,

    -- SNAPCHAT: Extract the ScCID from CONTEXT_PAGE_URL and store it in a new column SCCID
        CASE
            WHEN p.CONTEXT_PAGE_URL LIKE '%ScCid=%'
            THEN SPLIT_PART(SPLIT_PART(p.CONTEXT_PAGE_URL, 'ScCid=', 2), '&', 1)
            ELSE NULL
        END AS SCCID,

        ROW_NUMBER() OVER (PARTITION BY p.UUID ORDER BY p.TIMESTAMP DESC) AS rn

    FROM <<SCHEMA_NAME_GOES_HERE>>.PAGES p
    LEFT JOIN Identifiesranked i ON p.EXTERNAL_ID = i.EXTERNAL_ID AND i.rn = 1
    LEFT JOIN OrdersRanked o ON p.EXTERNAL_ID = o.EXTERNAL_ID AND o.rn = 1
    WHERE p.TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP)
)
SELECT *
FROM UUIDRanked
WHERE rn = 1
ORDER BY TIMESTAMP DESC;