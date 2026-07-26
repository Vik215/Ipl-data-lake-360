{
	"jobConfig": {
		"name": "ipl-gold-winning-margin-etl",
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
		"scriptName": "ipl-gold-winning-margin-etl.py",
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
		"createdOn": "2026-07-10T14:38:44.957Z",
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
	"script": "import sys\nfrom awsglue.transforms import *\nfrom awsglue.utils import getResolvedOptions\nfrom pyspark.context import SparkContext\nfrom awsglue.context import GlueContext\nfrom awsglue.job import Job\n\n## @params: [JOB_NAME]\nargs = getResolvedOptions(sys.argv, ['JOB_NAME'])\n\nsc = SparkContext()\nglueContext = GlueContext(sc)\nspark = glueContext.spark_session\njob = Job(glueContext)\njob.init(args['JOB_NAME'], args)\njob.commit()"
}