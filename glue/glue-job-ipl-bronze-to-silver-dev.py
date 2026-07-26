{
	"jobConfig": {
		"name": "glue-job-ipl-bronze-to-silver-dev",
		"description": "",
		"role": "arn:aws:iam::310971189049:role/iam-role-glue-ipl-etl-dev",
		"command": "glueetl",
		"version": "5.1",
		"runtime": null,
		"workerType": "G.1X",
		"numberOfWorkers": 10,
		"maxCapacity": 10,
		"jobRunQueuingEnabled": false,
		"maxRetries": 0,
		"timeout": 480,
		"maxConcurrentRuns": 1,
		"security": "none",
		"scriptName": "glue-job-ipl-bronze-to-silver-dev.py",
		"scriptLocation": "s3://aws-glue-assets-310971189049-ap-south-1/scripts/",
		"language": "python-3",
		"spark": true,
		"sparkConfiguration": "standard",
		"jobParameters": [
			{
				"key": "--conf",
				"value": "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true",
				"existing": false
			}
		],
		"tags": [],
		"jobMode": "DEVELOPER_MODE",
		"createdOn": "2026-07-09T09:12:33.404Z",
		"developerMode": true,
		"connectionsList": [],
		"temporaryDirectory": "s3://aws-glue-assets-310971189049-ap-south-1/temporary/",
		"logging": true,
		"glueHiveMetastore": true,
		"etlAutoTuning": true,
		"metrics": true,
		"observabilityMetrics": true,
		"bookmark": "job-bookmark-disable",
		"sparkPath": "s3://aws-glue-assets-310971189049-ap-south-1/sparkHistoryLogs/",
		"flexExecution": false,
		"minFlexWorkers": null,
		"maintenanceWindow": null
	},
	"hasBeenSaved": false,
	"usageProfileName": null,
	"script": "from pyspark.sql import SparkSession\r\nfrom pyspark.sql.functions import (\r\n    col,\r\n    year,\r\n    month,\r\n    when,\r\n    lit\r\n)\r\n# ----------------------------------------------------\r\n# Create Spark Session\r\n# ----------------------------------------------------\r\nspark = SparkSession.builder \\\r\n    .appName(\"IPL Bronze To Silver ETL\") \\\r\n    .getOrCreate()\r\n\r\nprint(\"===================================\")\r\nprint(\"IPL ETL Job Started\")\r\nprint(\"===================================\")\r\n\r\n# ----------------------------------------------------\r\n# Read Bronze CSV\r\n# ----------------------------------------------------\r\nbronze_path = \"s3://ipl-datalake-bronze-dev/matches/matches.csv\"\r\n\r\ndf = spark.read \\\r\n    .option(\"header\", \"true\") \\\r\n    .option(\"inferSchema\", \"true\") \\\r\n    .csv(bronze_path)\r\n\r\nprint(\"CSV Successfully Read\")\r\n\r\n# ----------------------------------------------------\r\n# Print Schema\r\n# ----------------------------------------------------\r\nprint(\"Schema\")\r\n\r\ndf.printSchema()\r\n\r\n# ----------------------------------------------------\r\n# Count Records\r\n# ----------------------------------------------------\r\nrow_count = df.count()\r\n\r\nprint(f\"Total Records : {row_count}\")\r\n\r\n# ----------------------------------------------------\r\n# Display Sample Records\r\n# ----------------------------------------------------\r\nprint(\"Sample Data\")\r\n\r\ndf.show(10, truncate=False)\r\n\r\nprint(\"===================================\")\r\nprint(\"Bronze Read Completed Successfully\")\r\nprint(\"===================================\")\r\n\r\n\r\n\r\n\r\n\r\n\r\nprint(\"Creating match_year column...\")\r\n\r\ndf = df.withColumn(\r\n    \"match_year\",\r\n    year(col(\"date\"))\r\n)\r\n\r\ndf.select(\"date\", \"match_year\").show(10, truncate=False)\r\n\r\n\r\nprint(\"Creating match_month column...\")\r\n\r\ndf = df.withColumn(\r\n    \"match_month\",\r\n    month(col(\"date\"))\r\n)\r\n\r\ndf.select(\"date\", \"match_year\", \"match_month\").show(10, truncate=False)\r\n\r\n\r\n# ==========================================\r\n# Create winner_flag column\r\n# ==========================================\r\n\r\nprint(\"Creating winner_flag column...\")\r\n\r\ndf = df.withColumn(\r\n    \"winner_flag\",\r\n    when(\r\n        col(\"winner\").isNull(),\r\n        \"No\"\r\n    ).otherwise(\"Yes\")\r\n)\r\n\r\n\r\ndf.select(\r\n    \"winner\",\r\n    \"winner_flag\"\r\n).show(10, truncate=False)\r\n\r\n\r\n\r\n# ==========================================\r\n# Create match_status column\r\n# ==========================================\r\n\r\nprint(\"Creating match_status column...\")\r\n\r\ndf = df.withColumn(\r\n    \"match_status\",\r\n    when(\r\n        col(\"winner\").isNull(),\r\n        \"No Result\"\r\n    ).otherwise(\"Completed\")\r\n)\r\n\r\n\r\ndf.select(\r\n    \"winner\",\r\n    \"match_status\"\r\n).show(10, truncate=False)\r\n\r\n\r\n\r\n# ==========================================\r\n# Create winning_margin_category column\r\n# ==========================================\r\n\r\nprint(\"Creating winning_margin_category column...\")\r\n\r\n\r\ndf = df.withColumn(\r\n    \"winning_margin_category\",\r\n    when(\r\n        col(\"result_margin\").isNull(),\r\n        \"Unknown\"\r\n    )\r\n    .when(\r\n        col(\"result_margin\").cast(\"int\") <= 5,\r\n        \"Close Match\"\r\n    )\r\n    .when(\r\n        col(\"result_margin\").cast(\"int\") <= 20,\r\n        \"Competitive\"\r\n    )\r\n    .otherwise(\r\n        \"Dominant Win\"\r\n    )\r\n)\r\n\r\n\r\ndf.select(\r\n    \"winner\",\r\n    \"result_margin\",\r\n    \"winning_margin_category\"\r\n).show(10, truncate=False)\r\n\r\n# ==========================================\r\n# Write Data to Silver Layer\r\n# ==========================================\r\n\r\nprint(\"===================================\")\r\nprint(\"Writing data to Silver layer\")\r\nprint(\"===================================\")\r\n\r\n\r\nsilver_path = \"s3://ipl-datalake-silver-dev/matches/\"\r\n\r\n\r\ndf.write \\\r\n    .mode(\"overwrite\") \\\r\n    .format(\"parquet\") \\\r\n    .save(silver_path)\r\n\r\n\r\nprint(\"===================================\")\r\nprint(\"Silver Load Completed Successfully\")\r\nprint(\"===================================\")\r\n\r\n\r\n\r\nspark.stop()"
}