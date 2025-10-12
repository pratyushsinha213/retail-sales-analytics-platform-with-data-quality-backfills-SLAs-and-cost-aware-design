-- Average Price by Category
CREATE VIEW dbo.avg_price_cat AS
SELECT
    c.category_key,
    c.category_name,
    ROUND(AVG(f.price), 2) AS avg_price
FROM dbo.fact_sales AS f
JOIN dbo.dim_category AS c
    ON f.category_key = c.category_key
GROUP BY
    c.category_key,
    c.category_name;

-- Monthly Sales Trend
CREATE VIEW dbo.monthly_sales_trend AS
SELECT
    FORMAT(TRY_CAST(f.sale_date AS DATE), 'yyyy-MM') AS month_year,
    SUM(f.revenue) AS total_revenue
FROM dbo.fact_sales AS f
GROUP BY
    FORMAT(TRY_CAST(f.sale_date AS DATE), 'yyyy-MM');


-- Top 10 Best Selling Products
CREATE VIEW dbo.top_10_best_selling_prods AS
SELECT
    p.product_key,
    p.product_name,
    SUM(f.quantity) AS total_quantity
FROM dbo.fact_sales AS f
JOIN dbo.dim_product AS p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY
    total_quantity DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


-- Total Sales Revenue by Category
CREATE VIEW dbo.total_sales_by_category
AS
SELECT
    dc.category_key,
    dc.category_name,
    SUM(fs.revenue) AS total_revenue
FROM dbo.dim_category AS dc
JOIN dbo.fact_sales AS fs
    ON dc.category_key = fs.category_key
GROUP BY
    dc.category_key,
    dc.category_name;


-- Total Sales by Country
CREATE VIEW dbo.total_sales_by_country AS
SELECT
    s.country,
    SUM(f.revenue) AS total_sales
FROM dbo.fact_sales AS f
JOIN dbo.dim_store AS s
    ON f.store_key = s.store_key
GROUP BY
    s.country;


-- Total Sales Revenue
CREATE VIEW dbo.total_sales_revenue AS
SELECT
    SUM(revenue) AS total_sales_revenue
FROM dbo.fact_sales;