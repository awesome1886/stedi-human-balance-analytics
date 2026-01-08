CREATE EXTERNAL TABLE `stedi`.`customer_landing`(
  `customername` string, 
  `email` string, 
  `phone` bigint, 
  `birthday` string, 
  `serialnumber` string, 
  `registrationdate` bigint, 
  `lastupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithpublicasofdate` bigint, 
  `sharewithfriendsasofdate` bigint)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://johndoe-stedi-lakehouse-v3/customer/landing/'
TBLPROPERTIES (
  'classification'='json');