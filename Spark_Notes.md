# Spark Notes 
**by: J. S. Ruiz**
## What is Spark ?
- Spark is a Developer Framework for big Data Process.
- It is focused in high velocity processing.
- It works with directed graphs (it needs to call an action before run the code).
## Languages
- **OLAP:** On-Line Analytical Processing (traditional on-line real time data bases)
- **OLTP:**  On-Line Transactional Processing (
- **NOTE:** Spark is focused to work in  data Warehouses or Data-lakes instead traditional databases.
- **NOTE:** Spark run over RAM memory to optimize velocity.
## History
- Spark is created in 2009 (Berkeley University)
- Is based on hadoop.
- Version 3 was lunched at Jun 10 2020.
## Spark vs Hadoop
- Spark was build to use RAM memory to process the data (optional: You can use spark directly with data storage in hard disks).
- Spark has native integration with ML (Machine Learning) , data streaming and graphs.
- It is not dependent of a file System (Example: You can use an AWS on the cloud and consume data from it).
## RDDs & Dataframes
### RDD (Resilient Distributed Dataset)
- Basic information unit.
- RDDs only support transformations and actions.
- RDDs are distributed along the entire cluster.
- they don't have specific structure, so they can adopt the best structure for the system.
- RDDs are incommutable, they can't be modify after they are created.
- They have better implementation. 
- You need to make an action to reflect the changes in the code using RDDs, before that changes are not saved and code is not running (Lazy elements).
### RDD Trnasformation
- It creates new RDDs from an existing RDDs.
### Dataframe
- They have column format.
- Dataframes are optimized for better performance.
- They are easier to be created.
## When use RDD instead dataframe?
- When you want to control a Spark flow.
- For python users, you can transform RDD into python native data.
- When you want to use Spark old versions.
## When use dataframe instead RDD?
- More complex data types.
- They have a well defined structure.
- They can be used in a high level tasks (filters, mapping, sums, averages, etc).
- They supports SQL query's.
## Spark Driver
- All Spark apps have a central program (Driver) and several executers (computers clusters).
- Executers run in parallel to complete the orders from the Driver and return a result.
- In a "stand alone model" Spark Driver and the executer are in the same computer (not in a cluster).
### Catalyst
- Spark optimization engine for task in execution. It is based on Scala.
- It is focused on big data optimization process (Integrated on Spark SQL architecture)
### Tungsten 
- Tungsten is a memory and CPU optimizer.
- Converts Catalyst logical plans into physical plans.

  
  









