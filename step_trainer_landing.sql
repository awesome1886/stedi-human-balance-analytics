CREATE EXTERNAL TABLE `stedi`.`step_trainer_landing`(
  `sensorreadingtime` bigint, 
  `serialnumber` string, 
  `distancefromobject` int)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://johndoe-stedi-lakehouse-v3/step_trainer/landing/'
TBLPROPERTIES (
  'classification'='json');