from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, year

# ✅ Initialize Spark Session
spark = SparkSession.builder.appName("Welmart Sales Analysis").getOrCreate()

# ✅ Load dataset (Ensure 'Superstore.csv' is in the same folder)
file_path = "Superstore.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ✅ Print Schema (For Debugging)
df.printSchema()

# ✅ Best-selling product sub-category
best_selling_sub_category = (
    df.groupBy("Sub-Category")
    .agg(sum("Quantity").alias("Total_Quantity"))
    .orderBy("Total_Quantity", ascending=False)
    .first()["Sub-Category"]
)

# ✅ Product category generating the highest revenue
highest_revenue_category = (
    df.groupBy("Category")
    .agg(sum("Sales").alias("Total_Sales"))
    .orderBy("Total_Sales", ascending=False)
    .first()["Category"]
)

# ✅ State with highest number of orders
state_with_most_orders = (
    df.groupBy("State")
    .agg(count("*").alias("Order_Count"))
    .orderBy("Order_Count", ascending=False)
    .first()["State"]
)

# ✅ Year with highest revenue
df = df.withColumn("Year", year(df["Order Date"]))
highest_revenue_year = (
    df.groupBy("Year")
    .agg(sum("Sales").alias("Total_Sales"))
    .orderBy("Total_Sales", ascending=False)
    .first()["Year"]
)

# ✅ Top 10 most valuable customers
top_customers = (
    df.groupBy("Customer ID")
    .agg(sum("Sales").alias("Total_Sales"))
    .orderBy("Total_Sales", ascending=False)
    .limit(10)
)

# ✅ Print Results
print("\n🔹 Best-selling product sub-category:", best_selling_sub_category)
print("🔹 Product category generating highest revenue:", highest_revenue_category)
print("🔹 State with highest number of orders:", state_with_most_orders)
print("🔹 Year with highest revenue:", highest_revenue_year)

print("\n🏆 Top 10 Most Valuable Customers:")
top_customers.show()

print("\n✅ Welmart analysis completed successfully!")

# ✅ Stop Spark Session
spark.stop()
