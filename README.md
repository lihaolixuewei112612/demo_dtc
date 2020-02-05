# dtc_bigdata_new
- 本项目主要功能：利用flink从kafka中消费指标数据，对数值型数据经过处理之后存储到opentsdb中；
对于字符串型数据则存储至hbase中。

以上两种数据都是供后台读取，然后实时告警


