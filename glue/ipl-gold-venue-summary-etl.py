{
	"jobConfig": {
		"name": "ipl-gold-venue-summary-etl",
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
		"scriptName": "ipl-gold-venue-summary-etl.py",
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
		"createdOn": "2026-07-10T14:37:34.960Z",
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
	"script": "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import count\nfrom pyspark.sql.functions import col, count\n\nspark = SparkSession.builder.getOrCreate()\n\ndf = spark.read.parquet(\"s3://ipl-datalake-silver-dev/matches/\")\n\nresult = (\n    df.groupBy(\"venue\")\n      .agg(count(\"*\").alias(\"matches_hosted\"))\n      .orderBy(col(\"matches_hosted\").desc())\n)\n\nresult.write.mode(\"overwrite\").parquet(\n    \"s3://ipl-datalake-gold-dev/venue_summary/\"\n)"
}