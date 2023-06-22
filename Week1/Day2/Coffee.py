# Databricks notebook source
# MAGIC %md
# MAGIC Question 1
# MAGIC Load in the dataset and have a look at it. What are the dimensions, variable types, variable names, etc.?

# COMMAND ----------

#

coffee= spark.read.csv("file:/Workspace/Repos/rashpal.singh@nrscotland.onmicrosoft.com/databricks_test/Week1/Day2/starbucks_drinkMenu_expanded.csv", header = True, inferSchema= True)

# COMMAND ----------

display(coffee)

# COMMAND ----------

print(f"The dataset contains {coffee.count()} rows and {len(coffee.columns)} columns")

# COMMAND ----------

coffee.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Question 2
# MAGIC Let’s first investigate the calories of different drinks. Select the variables Beverage_category, Beverage, Beverage prep and Calories from your data. Since we are interested in the calorie content, check if there are any NA values in the data, and drop them if there are.

# COMMAND ----------

coffee_subset = coffee.select("Beverage_category", "Beverage", "Beverage_prep", "Calories")

# COMMAND ----------

from pyspark.sql.functions import isNull

coffee_subset.select("Calories").isNull().count()

# COMMAND ----------

coffee_subset.select([F.count(F.when(F.isnan(c) | F.col(c).F.isNull(), c)).alias(c) for c in coffee_subset.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Question 3
# MAGIC Create a new variable (column) called calorie_diff, which stores the difference between 135 calories (135 calories = 10 cubes of sugar!) and the calories in each drink. (hint: you’ll want to subtract the calories from 135 to see which drink have more or less than 10 cups of sugar).

# COMMAND ----------

# MAGIC %md
# MAGIC Question 4
# MAGIC Summarise the mean number of calories in each beverage_category. Which 3 drinks have the most calories? Which 3 drinks have the least? Write a small summary of your findings.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Question 5
# MAGIC Let’s look at this a different way. What is the average number of calories in each Beverage_prep type?

# COMMAND ----------

# MAGIC %md
# MAGIC Question 6
# MAGIC Which Beverage_prep type contains more than the average calories of all drinks?
# MAGIC Hint: to answer this, you’ll have to first figure out what the average calories across all drinks are, and then use that as a filter for the grouped Beverage_prep data.

# COMMAND ----------

# MAGIC %md
# MAGIC Question 7
# MAGIC Which is the best type of coffee drink to get if you’re worried about consuming too many calories?

# COMMAND ----------


