-- creates view of month-day_of_week pairs with total payments above average

CREATE OR REPLACE VIEW Highest_Payment_Month_Day_Pairs AS 

WITH average_of_month_day_pairs AS (
	SELECT
		month_name,
		day_name_of_week,
		AVG("USD_payment_amount") AS average_payment_amount
	FROM public.toll_data
	GROUP BY
		month_name,
		day_name_of_week
)

SELECT
	month_name,
	day_name_of_week,
	SUM("USD_payment_amount") AS total_payment_amount

FROM public.toll_data

GROUP BY
	month_name,
	day_name_of_week
	
HAVING SUM("USD_payment_amount") > (SELECT MAX(average_payment_amount) FROM average_of_month_day_pairs)

;