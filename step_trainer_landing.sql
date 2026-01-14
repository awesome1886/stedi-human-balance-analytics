CREATE EXTERNAL TABLE `stedi`.`step_trainer_landing`(
  `sensorreadingtime` bigint, 
  `serialnumber` string, 
  `distancefromobject` int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://YOUR_BUCKET_NAME/step_trainer/landing/'
TBLPROPERTIES ('classification'='json');