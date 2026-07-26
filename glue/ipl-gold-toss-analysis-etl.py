{
	"jobConfig": {
		"name": "ipl-gold-toss-analysis-etl",
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
		"scriptName": "ipl-gold-toss-analysis-etl.py",
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
		"createdOn": "2026-07-10T14:38:19.512Z",
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
	"script": "import sys\nfrom awsglue.transforms import *\nfrom awsglue.utils import getResolvedOptions\nfrom pyspark.context import SparkContext\nfrom awsglue.context import GlueContext\nfrom awsglue.job import Job\nfrom pyspark.sql import SparkSession\nfrom pyspark.sql.functions import count\nfrom pyspark.sql.functions import col, count\nfrom pyspark.sql import SparkSession\nfrom pyspark.sql.functions import count\n\nspark = SparkSession.builder.getOrCreate()\n\ndf = spark.read.parquet(\"s3://ipl-datalake-silver-dev/matches/\")\n\nresult = (\n    df.groupBy(\"winning_margin_category\")\n      .agg(count(\"*\").alias(\"matches\"))\n      .orderBy(col(\"matches\").desc())\n)\n\nresult.write.mode(\"overwrite\").parquet(\n    \"s3://ipl-datalake-gold-dev/winning_margin_summary/\"\n)"
}