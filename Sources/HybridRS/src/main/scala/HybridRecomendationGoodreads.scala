package main.scala

import java.text.SimpleDateFormat
import java.util.Calendar
import com.datastax.spark.connector._
import com.datastax.spark.connector.{CassandraRow, UDTValue}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.Array._
import scala.collection.parallel.mutable
import scala.math._

/**
 * This class calculate the Featured Weighted User Model for the Goodreads.com dataset
 * Created by Simone Cogno on 11.12.15.
 */
object HybridRecomendationGoodreads {

  //Costants
  var nSimilarUsers = 20
  var nRecomendedItems = 40
  val DefaultFeatureValue = 1.0
  val MinShelveVotes = 10
  var TrainSetPercentage = 0.75
  val debug = false
  var local = false
  val SavePathS3 = "s3n://prs-simone/results/"
  val SavePathLocal = "/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/results/"
  val ReadPathS3 = "s3n://prs-simone/input/goodreads"
  val ReadPathLocal = "/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/goodreads"
  var defaultDriverMemoryMB = "13749m"
  var datasetName = "2016-1-15-05-22-05-477users0.1"

  def main(args: Array[String]): Unit = {
    
    //Program arguments (local, dataset name, number of similar users, number of items recommended, train set percentage )
    args match {
      case Array() =>
      case Array(l) => local = l.toBoolean
      case Array(l, dn) => {
        local = l.toBoolean
        datasetName = dn
      }
      case Array(l, dn, nu, ni) => {
        local = l.toBoolean
        datasetName = dn
        nSimilarUsers = nu.toInt
        nRecomendedItems = ni.toInt
      }
      case Array(l, dn, nu, ni, trainPerc) => {
        local = l.toBoolean
        datasetName = dn
        nSimilarUsers = nu.toInt
        nRecomendedItems = ni.toInt
        TrainSetPercentage=trainPerc.toDouble
      }
    }

    println("Local:"+local+" Dataset name:"+datasetName+" Simlar users: "+nSimilarUsers+" Recommended items: "+nRecomendedItems+" Train percentage"+TrainSetPercentage)


    //Spark configuration
    val conf = new SparkConf(true)
    if (local) {
      conf.set("spark.cassandra.connection.host", "CASSANDRA_IP") //Cassandra server configuration
          .set("spark.cassandra.input.fetch.size_in_rows", "10") //Limit fetch size
        .setAppName("Hybrid Algorithm")
        .setMaster("local[8]")
    } else {
      conf.setJars(Seq(System.getProperty("user.dir") + "/my-project-assembly.jar")) //Dependencies jar
    }

    //Create a Spark context
    val sc = new SparkContext(conf)


    var readPath = ""
    if (local) {
      readPath = ReadPathLocal
    } else {
      readPath = ReadPathS3
    }
      //Extract the user, items and shelves from the Goodreads Dataset
      val user_item_shelves: RDD[(Int, Array[(Int, Array[Int])])] = sc.objectFile(readPath + "/" + datasetName)
        .setName("user_item_shelves")
        .cache()

      val users_size = user_item_shelves.count()

      println("users size: " + users_size)


      //Split the rdd between train and test set
      val rdd_splitted: RDD[(Int, (Array[(Int, Array[Int])], Array[(Int, Array[Int])]))] = user_item_shelves.map { case (u, ib_is) =>
        val size: Int = ib_is.length
        var train: Array[(Int, Array[Int])] = Array()
        var test: Array[(Int, Array[Int])] = Array()
        ib_is.foreach { x =>
          if (scala.util.Random.nextDouble() < TrainSetPercentage) {
            train = train :+ x
          } else {
            test = test :+ x
          }
        }
        (u, (train, test))
      }
        .setName("rdd_splitted")
        .cache()

      //Test items
      val testItems = rdd_splitted.map{case (ugid, (vtr, vt))=> (ugid, vt.map(y=>y._1))} // (ugid, bgid)
          .setName("testItems")
          .cache()

      //Create list of user, item, reviews and list of shelves and retrieve their unique index
      val user_ff: RDD[(Int, (Int, Int))] = rdd_splitted.flatMap{case (u, (train, test))=>
        val shelves=train.flatMap(_._2)
        shelves.groupBy(x=>x).map(x=>(x._1, x._2.length))
                      .toArray
                      .map{case (s,ff)=> (s,(u, ff))}
      } //(u, (feature, f. frequency))
        .setName("user_ff")
        .cache()

      val user_ff_list = user_ff.map { case (s, (u, ff)) => (u, (s, ff)) }
        .groupByKey()

      //Calculate the user feature frequency in the whole dataset
      val ffw: RDD[(Int, Double)] = user_ff.map { case (s, (u, ff)) => (s, u) }
        .groupByKey()
        .map { case (s, s_u) => (s, log10(users_size.toDouble / s_u.toArray.distinct.length.toDouble)) }

      //Weight every features
      val user_ffw = user_ff.join(ffw).map { case (s, ((u, ff), fw)) =>
        (u, (s, fw * ff.toDouble))
      }.groupByKey()
        .setName("user_ffw")
        .cache()


      //Calculate the cosine similarities between users (u1, (u2, csim(u1,u2)))
      val similarities_pairs = user_ffw.cartesian(user_ffw)
        .filter { case ((u1, m1), (u2, m2)) => u1 != u2 }
        .map { case (((u1, a1), (u2, a2))) => (u1, (u2, cosineSimilarityList(a1.toList, a2.toList))) }
        .groupByKey()

      //Get the N most similar users
      val n_mostSimilarUsers = similarities_pairs.flatMap { case (u, sim_u) =>
        sim_u.toArray
          .sortBy(_._2).map(_._1)
          .reverse
          .take(nSimilarUsers)
          .map(simu => (simu, u))
      }.setName("n_mostSimilarUsers")
        .cache()

      //Get the items in the neighborhood (iu, Array[(ib, Iterable(is)]))
      val shelve_user_nbf: RDD[((Int, Int), Int)] = n_mostSimilarUsers.join(user_ff_list)
        .map { case (siu, (iu, is_ff)) => (iu, is_ff) }
        .groupByKey()
        .flatMap { case (iu, is_ff_a) =>
          val map_is_ff = is_ff_a.flatten
            .groupBy(_._1)
            .mapValues(_.map(_._2).sum)
            .map(identity)
          map_is_ff.map { case (s, nbf) =>
            ((s, iu), nbf)
          }
        }

      val nb_shelve_user_items = n_mostSimilarUsers
        .join(user_item_shelves)
        .flatMap { case (sim, (iu, ib_is)) =>
          ib_is.flatMap { case (ib, ais) =>
            ais.map(is => ((is, iu), ib))
          }
        }

      ///Get the N items that consists of features that are prevalent in the feature profiles of the user neighborhood
      val top_N_items: RDD[(Int, Array[(Int, Int)])] = nb_shelve_user_items.join(shelve_user_nbf).map{case ((is, iu), (ib, nbf))=>((iu, ib), nbf)}
                          .reduceByKey((nbf1, nbf2)=>nbf1+nbf2)//No more duplicates after reduce by key
                          .map{case ((u, b), nbw)=>(u, (b,nbw))}
                          .groupByKey()
                          .map { case (u, b_nbw) =>
                            (u, b_nbw.toArray
                              .sortBy(_._2)
                              .reverse
                              .take(nRecomendedItems))
                          }

      //Recommended items
      val results = top_N_items.map { case (u, lb_f) => (u, lb_f.map { case (b, ff) => b }) }
                                                                .setName("results")
                                                                .cache()

      //Accuracy of the prediction by users (True positive)
      val user_tp = results.join(testItems)  // (u, (Array[b_res], Array[b_test]))
                             .map{case (u,(b_res, b_test)) =>
                              println("res size:"+b_res.size+" test size"+b_test.size)
                              (u, b_res.intersect(b_test).length.toDouble)}
                              .setName("user_tp")
                              .cache()

      val user_fn = user_tp.join(testItems).map { case (u, (tp, b_test)) => (u, b_test.length - tp) }

      val user_fp = user_tp.join(results).map { case (uid, (tp, recom)) => (uid, recom.length - tp) }
        .setName("user_fp")
        .cache()

      //Precision tp/(tp+fp)
      val user_precision = user_tp.join(user_fp).map { case (uid, (tp, fp)) => (uid, tp / (tp + fp)) }
        .setName("user_precision")
        .cache()

      //Recall tp/(tp+fn)
      val user_recall = user_tp.join(user_fn).map { case (uid, (tp, fn)) =>
        val recall = tp / (tp + fn)
        if (recall.isNaN)
          (uid, 0.0)
        else
          (uid, recall)
      }.setName("user_recall")
        .cache()

      //F1-Score = (2*precision*recall) /(Precision+Recall)
      val user_f1_score = user_precision.join(user_recall).map { case (uid, (precision, recall)) =>
        val f1_score = (2.0 * precision * recall) / (precision + recall)
        if (f1_score.isNaN)
          (uid, 0.0)
        else
          (uid, f1_score)
      }.setName("user_f1_score")

      //Name of the output folder
      val format = new SimpleDateFormat("y-M-d-hh-mm-ss")
      val dateString = format.format(Calendar.getInstance().getTime)
      var ResultfolderName = ""
      if (local)
        ResultfolderName = ResultfolderName + SavePathLocal + dateString + "-" + users_size + "users-" + nRecomendedItems + "items" + nSimilarUsers + "sim"
      else
        ResultfolderName = ResultfolderName + SavePathS3 + dateString + "-" + users_size + "users" + nRecomendedItems + "items" + nSimilarUsers + "sim"
      //Save recomended items
      top_N_items.coalesce(100, false).flatMap{case (u, lb_f)=>lb_f.map(x=>(u,x._1)) }
                           .saveAsTextFile(ResultfolderName+"/recom")

      user_precision.coalesce(100, false).saveAsTextFile(ResultfolderName + "/user_precision")
      user_recall.coalesce(100, false).saveAsTextFile(ResultfolderName + "/user_recall")
      user_f1_score.coalesce(100, false).saveAsTextFile(ResultfolderName + "/user_f1_score")


    sc.stop()

    //Save the results to cassandra
    /*case class Recommendation(id: UUID, user_gid: Int, item_recommended: scala.collection.immutable.Vector[Int])
    val collection = sc.parallelize(Seq(Recommendation( UUID.randomUUID(), 23, Vector(1,2,3)),
                                        Recommendation( UUID.randomUUID(), 24, Vector(2,3,6))))



    collection.saveToCassandra("prs", "recommendation", SomeColumns("id", "user_gid", "item_recommended"))
    */
  }

  //Function for calculate the cosine similarity between two vector
  def cosineSimilarityVector(v1: Vector, v2: Vector): Double = {
    val v1_indexes: Array[Int] = v1.toSparse.indices
    val v2_indexes: Array[Int] = v2.toSparse.indices
    val indexes = v1_indexes.intersect(v2_indexes)
    if (indexes.length == 0) {
      0.0
    } else {
      val num = indexes.map(i => v1.apply(i) * v2.apply(i)).sum
      val v1_d = Math.sqrt(indexes.map(i => Math.pow(v1.apply(i), 2)).sum)
      val v2_d = Math.sqrt(indexes.map(i => Math.pow(v2.apply(i), 2)).sum)
      num / (v1_d * v2_d)
    }
  }
  //Function for calculate the cosine similarity between two Map
  def cosineSimilarityMap(m1: Map[Int, Double], m2: Map[Int, Double]): Double = {
    val m1_shelves: Array[Int] = m1.keys.toArray
    val m2_shelves: Array[Int] = m2.keys.toArray
    val common = m1_shelves.intersect(m2_shelves)
    if (common.length == 0) {
      0.0
    } else {
      val num = common.map(s => m1.getOrElse[Double](s, 0) * m2.getOrElse[Double](s, 0)).sum
      val m1_d = Math.sqrt(common.map(s => Math.pow(m1.getOrElse[Double](s, 0), 2)).sum)
      val m2_d = Math.sqrt(common.map(s => Math.pow(m2.getOrElse[Double](s, 0), 2)).sum)
      num / (m1_d * m2_d)
    }
  }
  //Function for calculate the cosine similarity between two List
  def cosineSimilarityList(a1: List[(Int, Double)], a2: List[(Int, Double)]): Double = {
    val common = a1.intersect(a2)
    if (common.isEmpty) {
      0.0
    } else {
      val common_values = common.map(s => (a1.find(_._1 == s._1).get._2, a2.find(_._1 == s._1).get._2))
      val num = common_values.map(x => x._1 + x._2).sum
      val m1_d = Math.sqrt(common_values.map(x => Math.pow(x._1, 2)).sum)
      val m2_d = Math.sqrt(common_values.map(x => Math.pow(x._2, 2)).sum)
      num / (m1_d * m2_d)
    }
  }
}
