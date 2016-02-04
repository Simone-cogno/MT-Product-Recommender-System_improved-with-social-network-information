
run-jar:	
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class "main.scala.TestSpark" \
	--master local[4] target/scala-2.10/simple-project_2.10-1.0.jar \
	--jars /Users/Simo/Downloads/spark-cassandra-connector_2.10-1.3.1.jar \
	/Users/Simo/Downloads/cassandra-driver-core-2.1.9.jar


assembly:
	sbt clean assembly

un:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class "main.scala.TrustWalkers" \
	--master local[4] target/scala-2.10/my-project-assembly.jar
run:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridCassandra \
	--master local[4] target/scala-2.10/my-project-assembly.jar
un_trust:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.TrustWalkers \
	--master local[4] target/scala-2.10/my-project-assembly.jar
run_hybrid_epinions:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridRecomendationEpinions \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt

hybrid_epinions_optimized:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridRecomendationEpinionsOptimized \
	--master local[8] target/scala-2.10/my-project-assembly.jar \
	true 2>&1 | tee log.txt

un_hybrid_with_evaluation:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridRecomendationWithEvaluation1 \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt
hybrid_with_evaluation_optimized:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridRecomendationWithEvaluation4 \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 \
	true 2016-1-14-10-40-15-19users 1024m | tee log.txt

copy_recom:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.PutRecommendationToCassandra \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt
stat:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.Statistics \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt
statistics:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.StatisticsSeparate \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt
statistics_summary:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.StatisticsSummary \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt	
shelve_stat:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.UserShelvesStatistics \
	--master local[8] target/scala-2.10/my-project-assembly.jar 1 true 2>&1 | tee log.txt	
trust:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.TrustWalkers_v1 \
	--master local[8] target/scala-2.10/my-project-assembly.jar \
	true 2>&1 | tee log.txt


copy_to_s3:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.CopyCassandraDatasetToS3 \
	--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt	
	

run-test:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.HybridCasandraTest \
	--master local[4] target/scala-2.10/my-project-assembly.jar

run-split:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class main.scala.dataset_split \
	--master local[4] target/scala-2.10/my-project-assembly.jar
brun:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/bin/spark-submit \
	--class "main.scala.TestSpark" \
	--master local[4] target/scala-2.10/simple-project_2.10-1.0.jar \
	--jars /Users/Simo/Downloads/spark-cassandra-connector_2.11-1.5.0-M2.jar \
	/Users/Simo/Downloads/cassandra-driver-core-2.1.9.jar

adsf:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/MT-Products-recommendation-system-using-the-social-media-networks/Code/Prototype1/target/scala-2.10/my-project-assembly.jar

launch:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/ec2/spark-ec2 \
	--key-pair=PRS-Simone \
	--identity-file=PRS-Simone.pem \
	--region=eu-west-1 \
	--zone=eu-west-1a  \
	--slaves=3 \
	--instance-type=r3.xlarge \
	launch \
	prs-spark-cluster
login:
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/ec2/spark-ec2 --key-pair=PRS-Simone --identity-file=PRS-Simone.pem --region=eu-west-1 --zone=eu-west-1c login prs-spark-cluster
	/Users/Simo/Google\ Drive/Master\ thesis/Document/spark1.5.1/ec2/spark-ec2 --key-pair=PRS-Simone --identity-file=PRS-Simone.pem --region=eu-west-1 --zone=eu-west-1c destroy prs-spark-cluster
launch_spark_job:
	./spark-submit \
	--conf spark.akka.frameSize=1000 \
	--class main.scala.TrustWalkers_v1 \
	--master spark://MASTER_IP:7077 my-project-assembly.jar \
	false 2>&1 | tee log.txt
