val matchRDD = sc.textFile("hdfs://lakki-VirtualBox:9000/demo/matches.csv")
val recRDD = matchRDD.filter(line=>{if(line.split(",").length<=18) true else false}).map(line=>line.split(",")).filter(rec=>rec(0)!="id")
val tupleRDD = recRDD.map(rec=>(rec(8),rec(11),rec(12),rec(14)))
val filterRDD = tupleRDD.filter(rec=>rec._2!="0").map(rec=>(rec._4,1)).reduceByKey((x,y)=>x+y).sortBy(-_._2)
val venueCnt = recRDD.map(rec=>(rec(14),1)).reduceByKey(_+_).sortBy(-_._2)
 filterRDD.join(venueCnt).map(rec=>(rec._1,(rec._2._1*100/rec._2._2))).map(key=>key.swap).sortByKey(ascending=false).collect

