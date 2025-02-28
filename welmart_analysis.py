from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, year, col

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

# ✅ Check if 'Order Type' column exists
if 'Order Type' in df.columns:
    # ✅ Analysis based on order types (single or bulk)
    order_type_analysis = (
        df.groupBy("Order Type")
        .agg(sum("Sales").alias("Total_Sales"), count("*").alias("Order_Count"))
        .orderBy("Total_Sales", ascending=False)
    )
    print("\n📊 Order Type Analysis:")
    order_type_analysis.show()
else:
    print("\n⚠️ 'Order Type' column not found in the dataset.")

# ✅ Analysis based on customer demographics (e.g., segment)
customer_segment_analysis = (
    df.groupBy("Segment")
    .agg(sum("Sales").alias("Total_Sales"), count("*").alias("Order_Count"))
    .orderBy("Total_Sales", ascending=False)
)

# ✅ Analysis based on shipping modes
shipping_mode_analysis = (
    df.groupBy("Ship Mode")
    .agg(sum("Sales").alias("Total_Sales"), count("*").alias("Order_Count"))
    .orderBy("Total_Sales", ascending=False)
)

# ✅ Print Results
print("\n🔹 Best-selling product sub-category:", best_selling_sub_category)
print("🔹 Product category generating highest revenue:", highest_revenue_category)
print("🔹 State with highest number of orders:", state_with_most_orders)
print("🔹 Year with highest revenue:", highest_revenue_year)

print("\n🏆 Top 10 Most Valuable Customers:")
top_customers.show()

print("\n📊 Customer Segment Analysis:")
customer_segment_analysis.show()

print("\n📊 Shipping Mode Analysis:")
shipping_mode_analysis.show()

print("\n✅ Welmart analysis completed successfully!")

# ✅ Stop Spark Session
spark.stop()
