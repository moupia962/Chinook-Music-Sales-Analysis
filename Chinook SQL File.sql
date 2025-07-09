use chinook;

SELECT 'customer' AS table_name, 'customer_id' AS column_name, COUNT(*) AS null_count 
FROM customer WHERE customer_id IS NULL
UNION ALL
SELECT 'customer', 'first_name', COUNT(*) FROM customer WHERE first_name IS NULL
UNION ALL
SELECT 'customer', 'email', COUNT(*) FROM customer WHERE email IS NULL
UNION ALL
SELECT 'invoice', 'invoice_id', COUNT(*) FROM invoice WHERE invoice_id IS NULL
UNION ALL
SELECT 'invoice', 'customer_id', COUNT(*) FROM invoice WHERE customer_id IS NULL
UNION ALL
SELECT 'track', 'track_id', COUNT(*) FROM track WHERE track_id IS NULL
UNION ALL
SELECT 'track', 'name', COUNT(*) FROM track WHERE name IS NULL
UNION ALL
SELECT 'employee', 'employee_id', COUNT(*) FROM employee WHERE employee_id IS NULL;


SELECT first_name, last_name, email, COUNT(*) AS duplicate_count
FROM customer
GROUP BY first_name, last_name, email
HAVING COUNT(*) > 1;

SELECT customer_id, invoice_date, total, COUNT(*) AS duplicate_count
FROM invoice
GROUP BY customer_id, invoice_date, total
HAVING COUNT(*) > 1;

SELECT first_name, last_name, email, COUNT(*) AS duplicate_count
FROM employee
GROUP BY first_name, last_name, email
HAVING COUNT(*) > 1;

SELECT name, album_id, media_type_id, COUNT(*) AS duplicate_count
FROM track
GROUP BY name, album_id, media_type_id
HAVING COUNT(*) > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM track
WHERE track_id NOT IN (
    SELECT track_id FROM (
        SELECT MIN(track_id) AS track_id
        FROM track
        GROUP BY name, album_id, media_type_id
    ) AS temp_table
);

SET SQL_SAFE_UPDATES = 1;

with detail as (select c.customer_id,
c.country,
a.album_id,
il.track_id,
t.name as track_name,
il.quantity,
ar.artist_id,
ar.name as artist_name,
g.genre_id,
g.name as genre_name
 from 
customer c
 join invoice i on c.customer_id=i.customer_id
 join invoice_line il on i.invoice_id=il.invoice_id
 join track t on il.track_id=t.track_id
 join album a on t.album_id=a.album_id
 join artist ar on a.artist_id=ar.artist_id
 join genre g on t.genre_id=g.genre_id)
 
 
 select
track_id, track_name,
count(track_id) as track_count,
country from detail
where country="USA"
group by track_id, track_name
order by track_count desc;

with detail as (select c.customer_id,

c.country,
a.album_id,
il.track_id,
t.name as track_name,
il.quantity,
ar.artist_id,
ar.name as artist_name,
g.genre_id,
g.name as genre_name
from
customer c
join invoice i on c.customer_id=i.customer_id
join invoice_line il on i.invoice_id=il.invoice_id
join track t on il.track_id=t.track_id
join album a on t.album_id=a.album_id
join artist ar on a.artist_id=ar.artist_id
join genre g on t.genre_id=g.genre_id),
artist_detail as

(select
artist_id,
artist_name,
album_id,
track_id,
count(quantity) as quantity_sold,
country
from detail
where country="USA"
group by track_id
order by quantity_sold desc)
select
artist_id,
artist_name,
sum(quantity_sold) as total_track_sold,
country
from artist_detail
group by artist_id, artist_name
order by total_track_sold desc;

SELECT
 genre_id,
 genre_name,
 sum(quantity) as total_quantity_sold from detail
 group by genre_id,genre_name
 order by total_quantity_sold desc;
 
 use chinook;

SELECT country, state, city, COUNT(customer_id) AS customer_count FROM customer
GROUP BY country, state, city
ORDER BY customer_count DESC;

use chinook;

SELECT
c.country,
COALESCE(c.state, 'Unknown') AS state,
c.city,
COUNT(i.invoice_id) AS total_invoices,
SUM(i.total) AS total_revenue
FROM invoice i
JOIN customer c ON i.customer_id=c.customer_id
GROUP BY c.country, c.state, c.city
ORDER BY total_revenue DESC;

use chinook;
WITH CustomerRevenue AS (
    SELECT 
        c.customer_id,
        concat(c.first_name, " " ,c.last_name) AS customer_name,
        c.country,
        SUM(i.total) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY SUM(i.total) DESC) AS `rank`
    FROM invoice i
    JOIN customer c ON i.customer_id = c.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.country
)
SELECT customer_id, customer_name, country, total_revenue,`rank`
FROM CustomerRevenue
WHERE `rank` <= 5
ORDER BY  country,`rank`;

use chinook;

WITH TrackSales AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        il.track_id,
        t.name AS track_name,
        SUM(il.quantity) AS total_quantity,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY SUM(il.quantity) DESC) AS `rank`
    FROM invoice i
    JOIN customer c ON i.customer_id = c.customer_id
    JOIN invoice_line il ON i.invoice_id = il.invoice_id
    JOIN track t ON il.track_id = t.track_id
    GROUP BY c.customer_id, c.first_name, c.last_name, il.track_id, t.name
)
SELECT customer_id, customer_name, track_id, track_name, total_quantity
FROM TrackSales
WHERE `rank` = 1
ORDER BY customer_id;

use chinook;

SELECT
c.customer_id,
CONCAT(c.first_name, c.last_name) AS customer_name,
COUNT(i.invoice_id) AS total_purchases,
MIN(i.invoice_date) AS first_purchase,
MAX(i.invoice_date) AS last_purchase,
ROUND(DATEDIFF(MAX(i.invoice_date), MIN(i.invoice_date)) / COUNT(i.invoice_id), 2) AS avg_days_between_purchases
FROM invoice i
JOIN customer c ON i.customer_id=c.customer_id
GROUP BY C.customer_id
ORDER BY total_purchases DESC;



SELECT
DATE_FORMAT(i.invoice_date, '%Y-%m') AS purchase_month,
COUNT(i.invoice_id) AS total_orders,
SUM(i.total) AS total_revenue
FROM invoice i
GROUP BY purchase_month;

use chinook;
WITH FirstYear AS (
    SELECT
        COUNT(DISTINCT customer_id) AS first_year_customers
    FROM invoice
    WHERE Invoice_date BETWEEN
        (SELECT MIN(invoice_date) FROM Invoice)
        AND DATE_ADD((SELECT MIN(invoice_date) FROM invoice), INTERVAL 1 YEAR)
),
LastYear AS (
    SELECT
        COUNT(DISTINCT customer_id) AS last_year_customers
    FROM Invoice
    WHERE Invoice_date BETWEEN
        DATE_SUB((SELECT MAX(Invoice_date) FROM Invoice), INTERVAL 1 YEAR)
        AND (SELECT MAX(invoice_date) FROM invoice)
)
SELECT
    (SELECT first_year_customers FROM FirstYear) AS first_year_customers,
    (SELECT last_year_customers FROM LastYear) AS last_year_customers,
    (SELECT first_year_customers FROM FirstYear) - (SELECT last_year_customers FROM LastYear) AS customer_differences;

use chinook;

WITH GenreSales AS (
SELECT
g.name AS genre_name,
SUM(il.unit_price * il.quantity) AS genre_sales
FROM invoice_line il
JOIN invoice i ON il.invoice_id= i.invoice_id
JOIN customer c ON i.customer_id = c.customer_id
JOIN track t ON il.track_id= t.track_id
JOIN genre g ON t.genre_id=g.genre_id
WHERE c.country ='USA'
GROUP BY g.name)
SELECT
gs.genre_name,
gs.genre_sales,
ROUND((gs.genre_sales/ (SELECT SUM(genre_sales) FROM GenreSales)) * 100, 2) As percentage_of_total_sales
FROM GenreSales gs
ORDER BY gs.genre_sales DESC;



use chinook;

SELECT
ar.name AS artist_name,
SUM(il.unit_price* il.quantity) AS artist_sales
FROM invoice_line il
JOIN invoice i ON il.invoice_id = i.invoice_id
JOIN customer c ON i.customer_id = c.customer_id
JOIN track t ON il.track_id = t.track_id
JOIN album al ON t.album_id= al.album_id
JOIN artist ar ON al.artist_id = ar.artist_id
WHERE c.country = 'USA'
GROUP BY ar.name
ORDER BY artist_sales DESC
LIMIT 10;



use chinook;

SELECT
c.customer_id,
CONCAT(c.first_name,
c.last_name) AS customer_name,
COUNT(DISTINCT g.genre_id) AS genre_count
FROM invoice i
JOIN customer c ON i.customer_id = c.customer_id
JOIN invoice_line il ON i.invoice_id =il.invoice_id
JOIN track t ON il.track_id = t.track_id
JOIN genre g ON t.genre_id= g.genre_id
GROUP BY c.customer_id
HAVING genre_count >= 3
ORDER BY genre_count DESC;


use chinook;

SELECT
g.name AS genre_name,
SUM(il.unit_price* il.quantity) AS total_sales, RANK() OVER (ORDER BY SUM(il.unit_price* il.quantity) DESC) AS sales_rank FROM invoice_line il
JOIN invoice i ON il.invoice_id=i.invoice_id
JOIN customer c ON i.customer_id = c.customer_id
JOIN track t ON il.track_id= t.track_id
JOIN genre g ON t.genre_id= g.genre_id
WHERE c.country= "USA"
GROUP BY g.genre_id
ORDER BY total_sales DESC;


use chinook;

SELECT
c.customer_id,
CONCAT(c.first_name, '', c.last_name) AS customer_name,
MAX(i.invoice_date) AS last_purchase_date
FROM customer c
LEFT JOIN invoice i ON c.customer_id= i.customer_id
GROUP BY c.customer_id
HAVING last_purchase_date < DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
OR last_purchase_date IS NULL
ORDER BY last_purchase_date;

use chinook;
SELECT g.name AS genre, SUM(il.unit_price * il.quantity) AS total_sales
FROM invoice i
JOIN customer c ON i.customer_id = c.customer_id
JOIN invoice_line il ON i.invoice_id = il.invoice_id
JOIN track t ON il.track_id = t.track_id
JOIN genre g ON t.genre_id = g.genre_id
WHERE c.country = 'USA'
GROUP BY g.genre_id
ORDER BY total_sales ASC 
LIMIT 3;


SELECT a.album_id, a.title, ar.name AS artist, SUM(il.unit_price * il.quantity) AS album_sales
FROM invoice i
JOIN customer c ON i.customer_id = c.customer_id
JOIN invoice_line il ON i.invoice_id = il.invoice_id
JOIN track t ON il.track_id = t.track_id
JOIN album a ON t.album_id = a.album_id
JOIN artist ar ON a.artist_id = ar.artist_id
JOIN genre g ON t.genre_id = g.genre_id
WHERE c.country = 'USA' AND g.name IN ('TV Shows', 'Soundtrack', 'Heavy Metal') 
GROUP BY a.album_id
ORDER BY album_sales ASC  
LIMIT 3;

use chinook;

SELECT g.name AS genre, SUM(il.unit_price * il.quantity) AS total_sales
FROM invoice i
JOIN customer c ON i.customer_id = c.customer_id
JOIN invoice_line il ON i.invoice_id = il.invoice_id
JOIN track t ON il.track_id = t.track_id
JOIN genre g ON t.genre_id = g.genre_id
WHERE c.country <> 'USA'
GROUP BY g.genre_id
ORDER BY total_sales DESC
LIMIT 3;

use chinook;

SELECT c.customer_id, c.first_name, c.last_name,
MIN(i.invoice_date) AS first_purchase_date,
max(i.invoice_date) as last_purchase_date,
count(i.invoice_date) as purchase_count
FROM customer c
JOIN invoice i ON c.customer_id = i.customer_id
GROUP BY c.customer_id
order by purchase_count;




WITH PurchaseGaps AS (
    SELECT 
        i.customer_id,
        DATEDIFF(i.invoice_date, 
                 LAG(i.invoice_date) OVER (PARTITION BY i.customer_id ORDER BY i.invoice_date)) / 30 AS repurchase_gap_months
    FROM invoice i
),
AverageRepurchase AS (
    SELECT 
        p.customer_id,
        ROUND(AVG(p.repurchase_gap_months), 2) AS avg_repurchase_interval
    FROM PurchaseGaps p
    WHERE p.repurchase_gap_months IS NOT NULL 
    GROUP BY p.customer_id
)
SELECT 
    ar.customer_id,
    ar.avg_repurchase_interval,
    CASE 
        WHEN ar.avg_repurchase_interval <= 3 THEN 'Frequent Customer'
        ELSE 'Non-Frequent Customer'
    END AS customer_category
FROM AverageRepurchase ar;


WITH PurchaseGaps AS (
    SELECT 
        i.customer_id,
        DATEDIFF(i.invoice_date, 
                 LAG(i.invoice_date) OVER (PARTITION BY i.customer_id ORDER BY i.invoice_date)) / 30 AS repurchase_gap_months
    FROM invoice i
),
CustomerCategory AS (
    SELECT 
        p.customer_id,
        ROUND(AVG(p.repurchase_gap_months), 2) AS avg_repurchase_interval,
        CASE 
            WHEN ROUND(AVG(p.repurchase_gap_months), 2) <= 3 THEN 'Frequent Customer'
            ELSE 'Non-Frequent Customer'
        END AS customer_type
    FROM PurchaseGaps p
    WHERE p.repurchase_gap_months IS NOT NULL
    GROUP BY p.customer_id
)
SELECT 
    cc.customer_type,
    COUNT(DISTINCT i.customer_id) AS total_customers,
    SUM(i.total) AS total_spending,
    ROUND(AVG(i.total), 2) AS avg_spending_per_invoice,
    ROUND(SUM(i.total) / COUNT(DISTINCT i.customer_id), 2) AS avg_spending_per_customer
FROM invoice i
JOIN CustomerCategory cc ON i.customer_id = cc.customer_id
GROUP BY cc.customer_type;



WITH PurchaseGaps AS (
    SELECT 
        i.customer_id,
        DATEDIFF(i.invoice_date, 
                 LAG(i.invoice_date) OVER (PARTITION BY i.customer_id ORDER BY i.invoice_date)) / 30 AS repurchase_gap_months
    FROM invoice i
),
CustomerCategory AS (
    SELECT 
        p.customer_id,
        ROUND(AVG(p.repurchase_gap_months), 2) AS avg_repurchase_interval,
        CASE 
            WHEN ROUND(AVG(p.repurchase_gap_months), 2) <= 3 THEN 'Frequent Customer'
            ELSE 'Non-Frequent Customer'
        END AS customer_type
    FROM PurchaseGaps p
    WHERE p.repurchase_gap_months IS NOT NULL
    GROUP BY p.customer_id
)
SELECT 
    cc.customer_type,
    ROUND(AVG(il.quantity), 2) AS avg_items_per_order
FROM invoice_line il
JOIN invoice i ON il.invoice_id = i.invoice_id
JOIN CustomerCategory cc ON i.customer_id = cc.customer_id
GROUP BY cc.customer_type;

use chinook;

WITH GenrePairs AS (
    SELECT 
        il1.invoice_id,
        g1.name AS genre_1,
        g2.name AS genre_2
    FROM invoice_line il1
    JOIN track t1 ON il1.track_id = t1.track_id
    JOIN genre g1 ON t1.genre_id = g1.genre_id
    JOIN invoice_line il2 ON il1.invoice_id = il2.invoice_id AND il1.track_id < il2.track_id
    JOIN track t2 ON il2.track_id = t2.track_id
    JOIN genre g2 ON t2.genre_id = g2.genre_id
)
SELECT 
    genre_1, 
    genre_2, 
    COUNT(*) AS frequency
FROM GenrePairs
GROUP BY genre_1, genre_2
ORDER BY frequency DESC
LIMIT 10;



WITH AlbumPairs AS (
    SELECT 
        il1.invoice_id,
        a1.title AS album_1,
        a2.title AS album_2
    FROM invoice_line il1
    JOIN track t1 ON il1.track_id = t1.track_id
    JOIN album a1 ON t1.album_id = a1.album_id
    JOIN invoice_line il2 ON il1.invoice_id = il2.invoice_id AND il1.track_id < il2.track_id
    JOIN track t2 ON il2.track_id = t2.track_id
    JOIN album a2 ON t2.album_id = a2.album_id
)
SELECT 
    album_1, 
    album_2, 
    COUNT(*) AS frequency
FROM AlbumPairs
GROUP BY album_1, album_2
ORDER BY frequency DESC
LIMIT 10;



WITH ArtistPairs AS (
    SELECT 
        il1.invoice_id,
        ar1.name AS artist_1,
        ar2.name AS artist_2
    FROM invoice_line il1
    JOIN track t1 ON il1.track_id = t1.track_id
    JOIN album a1 ON t1.album_id = a1.album_id
    JOIN artist ar1 ON a1.artist_id = ar1.artist_id
    JOIN invoice_line il2 ON il1.invoice_id = il2.invoice_id AND il1.track_id < il2.track_id
    JOIN track t2 ON il2.track_id = t2.track_id
    JOIN album a2 ON t2.album_id = a2.album_id
    JOIN artist ar2 ON a2.artist_id = ar2.artist_id
)
SELECT 
    artist_1, 
    artist_2, 
    COUNT(*) AS frequency
FROM ArtistPairs
GROUP BY artist_1, artist_2
ORDER BY frequency DESC
LIMIT 10;

use chinook;

SELECT 
    c.country,
    COUNT(DISTINCT i.customer_id) AS total_customers,
    SUM(i.total) AS total_revenue,
    ROUND(AVG(i.total), 2) AS avg_spending_per_invoice,
    ROUND(SUM(i.total) / COUNT(DISTINCT i.customer_id), 2) AS avg_spending_per_customer
FROM invoice i
JOIN customer c ON i.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;

use chinook;

WITH LocationSpending AS (
SELECT
c.country,
COUNT(i.invoice_id) AS total_purchases,
SUM(i.total) AS total_revenue,
COUNT(DISTINCT c.customer_id) AS total_customers,
ROUND(SUM(i.total) / COUNT(DISTINCT c.customer_id), 2) AS avg_spent_per_customer,
MAX(i.invoice_date) AS last_purchase_date
FROM customer c
JOIN invoice i ON c.customer_id= i.customer_id
GROUP BY c.country)
SELECT
country,
total_customers,
case
when total_customers<=3 then "High_Rish"
when total_customers<=7 then "Moderate_Risk"
else "Low_Rish"
end as Risk_type,
total_revenue,
avg_spent_per_customer
FROM LocationSpending
ORDER BY
total_customers asc, total_purchases asc, avg_spent_per_customer asc,total_revenue asc;



WITH PurchaseHistory AS (
    SELECT 
        i.customer_id,
        i.invoice_date,
        LAG(i.invoice_date) OVER (PARTITION BY i.customer_id ORDER BY i.invoice_date) AS prev_purchase_date,
        DATEDIFF(i.invoice_date, LAG(i.invoice_date) OVER (PARTITION BY i.customer_id ORDER BY i.invoice_date)) AS days_between_purchases,
        i.total
    FROM invoice i
),
PurchaseBehavior AS (
    SELECT 
        ph.customer_id,
        COUNT(ph.invoice_date) AS total_purchases,
        ROUND(SUM(ph.total), 2) AS total_purchase_amount,
        ROUND(AVG(ph.total), 2) AS avg_spent_per_purchase,
        ROUND(AVG(ph.days_between_purchases), 2) AS avg_days_between_purchases
    FROM PurchaseHistory ph
    GROUP BY ph.customer_id
)
SELECT 
    pb.customer_id,
    pb.total_purchases,
    pb.total_purchase_amount,
    pb.avg_spent_per_purchase,
    pb.avg_days_between_purchases
FROM PurchaseBehavior pb
ORDER BY pb.avg_spent_per_purchase asc, pb.total_purchase_amount asc, total_purchases asc,
pb.avg_days_between_purchases desc;


use chinook;

SELECT 
    c.country, 
    COUNT(DISTINCT c.customer_id) AS total_customers,
    ROUND(SUM(i.total) / COUNT(DISTINCT c.customer_id), 2) AS avg_total_spent_per_customer,
    ROUND(SUM(il.quantity) / COUNT(DISTINCT c.customer_id), 2) AS avg_tracks_per_customer
FROM customer c
JOIN invoice i ON c.customer_id = i.customer_id
JOIN invoice_line il ON i.invoice_id = il.invoice_id
GROUP BY c.country
ORDER BY avg_total_spent_per_customer DESC;

use chinook;

ALTER TABLE album
add COLUMN ReleaseYear INTEGER;

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    TIMESTAMPDIFF(MONTH, MIN(i.invoice_date), CURDATE()) AS tenure_months,
    SUM(i.total) AS total_purchase_amount,
    COUNT(i.invoice_id) AS purchase_frequency,
    CASE 
        WHEN SUM(i.total) >= 60 THEN 'High-Value'
        WHEN SUM(i.total) >= 40 THEN 'Mid-Value'
        ELSE 'Low-Value'
    END AS customer_segment
FROM 
    customer c
JOIN 
    invoice i ON c.customer_id = i.customer_id
GROUP BY 
    c.customer_id, c.first_name, c.last_name
ORDER BY 
    total_purchase_amount DESC;
    
    WITH customer_summary AS (
    SELECT 
        c.customer_id,
        SUM(i.total) AS total_purchase_amount,
        COUNT(i.invoice_id) AS purchase_frequency,
        CASE 
            WHEN SUM(i.total) >= 60 THEN 'High-Value'
            WHEN SUM(i.total) >= 40 THEN 'Mid-Value'
            ELSE 'Low-Value'
        END AS customer_segment
    FROM 
        customer c
    JOIN 
        invoice i ON c.customer_id = i.customer_id
    GROUP BY 
        c.customer_id
)

SELECT 
    customer_segment,
    ROUND(AVG(total_purchase_amount / purchase_frequency), 2) AS avg_spend_per_purchase
FROM 
    customer_summary
GROUP BY 
    customer_segment
ORDER BY 
    avg_spend_per_purchase DESC;






