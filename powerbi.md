# Infrabel Train Punctuality Project

<br>


## Creating Reports and Dashboard Templates (Power BI)


### A. Dashboard Overview

The final Power BI report includes several pages, each focused on a different aspect of train punctuality:

- **Executive Overview:** KPI cards (On-Time Performance, Average Arrival/Departure Delay, Total Trains, etc.) and a line chart showing monthly trends.

![Overview](/screenshots/report%2001.png "Overview")

- **Temporal Analysis:** Detailed analysis of delays by day, hour, and quarter.

![Timetrends](/screenshots/report%2002.png "Timetrends")

These pages allow users to slice and dice the data dynamically using filters such as date ranges, train operator, station, and time of day.

### B. Connecting Power BI to AWS Athena: A Step-by-Step Guide

Below is the detailed process I followed based on best practices and guided by Matthew Yu [video](https://www.youtube.com/watch?v=FKdCr6vmq-o) tutorials to connect Power BI to our AWS Athena data warehouse:

#### 1. ***Install the AWS Athena ODBC Driver***
- **Download:** Visit the [AWS Athena ODBC driver page](https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html) and download the appropriate driver for your operating system.
- **Install:** Run the installer and follow the instructions to install the driver on your machine.

#### 2. ***Configure the ODBC Data Source (DSN)***
- **Open ODBC Data Source Administrator:** On your Windows machine, open the "ODBC Data Source Administrator" (choose the 64-bit version if using Power BI Desktop 64-bit).
- **Add a New DSN:**
  - Click on the "System DSN" or "User DSN" tab, then click "Add."
  - Select the **AWS Athena ODBC Driver** from the list.
- **Configure DSN Settings:**
  - **Data Source Name:** Provide a meaningful name (e.g., `Athena_DS`).
  - **AWS Region:** Enter the region where your Athena database is hosted (e.g., `eu-west-1` or the appropriate region).
  - **S3 Staging Directory:** Provide the S3 bucket path where Athena stores query results (e.g., `s3://your-athena-output-bucket/path/`).
  - **Authentication:** Enter your AWS Access Key and Secret Key or configure your profile if supported.
  - **Advanced Settings:** You may also set additional parameters if needed (timeout, SSL settings, etc.).
- **Test the DSN:** Use the "Test" button (if available) to ensure that the connection is successful.

#### 3. ***Connect Power BI Desktop to AWS Athena***
- **Launch Power BI Desktop:** Open Power BI Desktop on your machine.
- **Get Data:**  
  - Click on **Home > Get Data**.
  - In the Get Data window, search for or select **ODBC** as the data source.
- **Select DSN:**
  - In the ODBC connector window, choose the DSN you just configured (`Athena_DS`).
  - Click **OK**.
- **Navigator Window:**  
  - Power BI will retrieve the list of available tables from your Athena database.
  - Select the tables (or entire database) you need for your dashboard (e.g., FactTrain, DimDate, DimTrain, DimStation).
  - Click **Load** or **Transform Data** if you need to perform additional data shaping in Power Query.

#### 4. ***Build the Data Model***
- **Relationships:**  
  - In the Power BI Model view, verify that the relationships between your fact and dimension tables are correctly established. Adjust as necessary.
- **Calculated Columns and Measures:**  
  - Create calculated columns or DAX measures (such as Average Delay, On-Time Performance, etc.) to support your analysis.
- **Data Refresh:**  
  - Configure scheduled refresh settings in Power BI Service (if publishing the report) to ensure that the dashboard reflects up-to-date data from Athena.

#### 5. ***Create the Dashboard***
- **Design the Layout:**  
  - Using the Power BI canvas, design your pages as outlined in the Overview. Use a consistent color palette and clear fonts.
- **Add Visuals:**  
  - Insert KPI cards, line charts, bar/column charts, maps, and tables to represent key performance indicators and trends.
- **Interactivity:**  
  - Add slicers for Date, Train Operator, and Station to allow users to filter the data dynamically.
- **Final Touches:**  
  - Add titles, tooltips, and descriptive text boxes to explain each visualization’s purpose.

#### 6. ***Publish and Share***
- **Publish:**  
  - Once the report is complete, publish it to the Power BI Service.
- **Sharing and Collaboration:**  
  - Configure user access, dashboards, and report sharing options so that stakeholders can view and interact with the dashboard.


![Dashboard](/screenshots/dashbord%2001.png "Dashboard")

