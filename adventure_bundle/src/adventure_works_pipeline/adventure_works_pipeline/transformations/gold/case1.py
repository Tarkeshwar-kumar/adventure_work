'''
Revenue each product category generates across sales territories.
'''

import dlt

@dlt.table(
    name="terriory_revenue_by_category_gold"
)
def territory_revenue_by_category():
    sales = spark.read.table("adventure_work.silver.fact_sales_silver")
    products = spark.read.table("adventure_work.silver.dim_product_silver")
    subcategories = spark.read.table("adventure_work.silver.dim_subcategories_silver")
    categories = spark.read.table("adventure_work.silver.dim_customers_silver")
    territories = spark.read.table("adventure_work.silver.dim_territories_silver")
    sales_products = (sales
    .join(products, "ProductKey", "left")
    .join(subcategories, "ProductSubcategoryKey", "left")
    .join(categories, "ProductCategoryKey", "left"))

# Join with territories
    sales_with_region = sales_products.join(territories, "SalesTerritoryKey", "left")

# Add revenue column
    sales_with_region = sales_with_region.withColumn(
        "Revenue", sales_with_region.OrderQuantity * sales_with_region.ProductPrice
    )
    regional_category_perf = (sales_with_region
        .groupBy("RegionName", "CategoryName")
        .agg(_sum("Revenue").alias("TotalRevenue"))
        .orderBy("TotalRevenue", ascending=False))
    
    return regional_category_perf