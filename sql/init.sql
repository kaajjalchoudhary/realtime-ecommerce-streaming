CREATE TABLE IF NOT EXISTS window_metrics (
    window_start     TIMESTAMPTZ PRIMARY KEY,
    window_end       TIMESTAMPTZ   NOT NULL,
    page_views       INTEGER       NOT NULL,
    add_to_carts     INTEGER       NOT NULL,
    purchases        INTEGER       NOT NULL,
    revenue          NUMERIC(12,2) NOT NULL,
    unique_users     INTEGER       NOT NULL,
    conversion_rate  NUMERIC(6,4)  NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Handy rollup for dashboards
CREATE OR REPLACE VIEW hourly_metrics AS
SELECT
    date_trunc('hour', window_start)                          AS hour,
    SUM(page_views)                                           AS page_views,
    SUM(add_to_carts)                                         AS add_to_carts,
    SUM(purchases)                                            AS purchases,
    SUM(revenue)                                              AS revenue,
    ROUND(SUM(purchases)::numeric / NULLIF(SUM(page_views), 0), 4) AS conversion_rate
FROM window_metrics
GROUP BY 1;
