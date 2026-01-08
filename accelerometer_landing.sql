CREATE EXTERNAL TABLE `stedi`.`accelerometer_landing`(
  `user` string, 
  `timestamp` bigint, 
  `x` float, 
  `y` float, 
  `z` float)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://johndoe-stedi-lakehouse-v3/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json');