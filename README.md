# Big Data Technologies in AWS

## Introduction

In today's data-driven world, leveraging Big Data technologies is essential for gaining actionable insights and making informed decisions. This repository explores key Big Data technologies within the AWS ecosystem, providing examples and use cases to demonstrate their power and versatility.

## Big Data Technologies Covered

### Amazon DynamoDB
- **Description**: A fully managed NoSQL database service that delivers fast and predictable performance with seamless scalability. It is ideal for applications requiring consistent, low-latency data access.
- **Use Cases**: Real-time data processing, mobile and web applications, IoT data storage, and metadata management.

### Amazon Redshift
- **Description**: A fully managed data warehouse service that enables you to run complex queries on large datasets using SQL. It integrates with various BI tools for advanced analytics.
- **Use Cases**: Data warehousing, business intelligence, reporting, and large-scale data analytics.

### Amazon EMR (Elastic MapReduce)
- **Description**: A cloud-based big data platform that simplifies running big data frameworks like Hadoop and Spark. EMR allows for scalable and cost-effective data processing.
- **Use Cases**: Large-scale data processing, ETL (Extract, Transform, Load) operations, log analysis, and machine learning.

### Apache Spark on AWS
- **Description**: An open-source distributed computing system that provides an interface for programming entire clusters. Spark on AWS EMR supports large-scale data processing and real-time stream processing.
- **Use Cases**: Iterative algorithms, interactive data analysis, real-time data processing, and big data analytics.
- **Update: StructuredStremAnalysis.py:**

  The PySpark framework, “StructuredStreamingWindowAggregation” is centered around processing streaming JSON data from a local folder. The data we’re processing represents activity logs with fields such as 'Arrival_Time', 'gt', 'x', 'y', and 'z'. The objective here is to perform time-windowed aggregations on this data, specifically calculating the counts and mean values of 'x', 'y', and 'z', grouped by the 'gt' field, and then offer recommendations based on some conditions.
  Structured streaming helps to treat live data streams of the activity data JSON files as tables to which incoming data is continuously appended. We apply window operations to group data based on event time windows, these tables are appended to in batches. In this framework implementation, we process the stream for three distinct window durations: 5 minutes, 15 minutes, and 30 minutes. We generate aggregate metrics like counts and mean values for each window, which helps to observe trends over different time intervals, which in turn allows us to recommend some action. The process_stream() function maintains state across different windows, enabling us to compare metrics between consecutive time frames.
  It provides recommendations like "Standing recommended" and "Move recommended," based on the counts of 'sit' and 'stand' and the movement distance calculated from the mean values of 'x', 'y', and 'z'.
  By combining windowed aggregations and stateful computations, our PySpark structured streaming framework offers valuable insights into activity patterns with different times within the streaming data. This framework demonstrates the potential of PySpark in transforming raw, continuous data streams into actionable intelligence, making it an important element in the big data analytics field.
  It is extremely applicable to the wearable technology industry, providing the user with real-time analytics and important health reminders based on their behaviors. By analyzing data from wearable devices like fitness trackers, the framework can monitor physical activities (like walking, running, sitting, standing) in real time. It can provide immediate feedback to users, encouraging more active lifestyles.

## How to Use

1. **Clone the Repository**
   - Clone this repository to your local machine using the following command:
     ```bash
     git clone https://github.com/owgee/big-data.git
     ```

2. **Set Up AWS Environment**
   - Ensure you have an AWS account and have set up the necessary services (e.g., DynamoDB, Redshift, EMR).
   - Configure your AWS credentials and settings as required.

3. **Run the Analysis**
   - Follow the instructions within each notebook to run the analyses. Ensure you have the necessary libraries and AWS services configured.

## Conclusion

This repository provides a hands-on exploration of various Big Data technologies within the AWS ecosystem. By leveraging these tools, you can efficiently process, analyze, and extract insights from large datasets, driving better decision-making and optimizing your data workflows.

---

Feel free to explore, modify, and enhance the analyses as per your requirements!

**NOTE:** 
I will be updating this Readme and the repo regularly until I'm done with all the work I have done
