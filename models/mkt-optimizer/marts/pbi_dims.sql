SELECT DISTINCT cast(history as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY history) id,							'history'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(photo as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY photo) id,								'photo'					AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(gearBox as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY gearBox) id,							'gearBox'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(fuelType as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY fuelType) id,							'fuelType'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(alternativeFuelType as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY alternativeFuelType) id,	'alternativeFuelType'	AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(drive as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY drive) id,								'drive'					AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(body as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY body) id,									'body'					AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(KM_Range as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY KM_Range) id,							'KM Range'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(ProductionYear as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY ProductionYear) id,				'ProductionYear'		AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(Category as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Category) id,							'Category'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(Buy_source as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Buy_source) id,						'Buy_source'			AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(model as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY model) id,								'model'					AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(Make as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Make) id,									'make'					AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(Make_model as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Make_model) id,						'Make_model'			AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
--SELECT DISTINCT cast(Branch as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Branch) id,								'Branch'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(Country as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY Country) id,							'Country'				AS cat FROM {{ ref('fact_car_tech_info') }} UNION ALL
SELECT DISTINCT cast(PriceRange as varchar(255))  AS param, DENSE_RANK() OVER (ORDER BY PriceRange) id,						'PriceRange'			AS cat FROM {{ ref('fact_car_tech_info') }} 

