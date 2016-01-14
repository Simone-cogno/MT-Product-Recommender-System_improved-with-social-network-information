package main.scala

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.Array._
import scala.math._

/**
 * This Spark application perform an Hybrid recommendation by using information about product
 * retrieved from Goodreads.com and epinions.com.
 *
 * Created by Simone Cogno on 11.12.15.
 */
object HybridRecomendation {

  //Costants definition
  val MinUsersSimilarity = 0.3 //Minimum similarity between user neighborhood
  val NSimilarUsers=15         //Size of the Neighborhood
  val NRecemendedItems=50      //Number of recommended items
  val DefaultFeatureValue=1.0
  val MinShelveVotes=5         //Minimum user rating for a shelve to take it into account
  val TrainSetPercentage=0.75
  val Debug=false              //Print debug informations
  val Local=true               //Application will run in the local machine
  val SavePathS3="s3n://prs-simone/results/"  //Default path for save the results of the computation
  val SavePathLocal="/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/results/"
  val ReadPathS3="s3n://prs-simone/input/"    //Default path for read the input dataset
  val ReadPathLocal="/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/goodreads"

  def main(args: Array[String]): Unit = {
    //Spark configuration
    val conf = new SparkConf(true).setAppName("Hybrid Algorithm")
    if(Local) {
      conf.setMaster("local[8]")
    }else{
      conf.setMaster("spark://MASTER-IP:7077")
          .setJars(Seq(System.getProperty("user.dir") + "/my-project-assembly.jar")) //Dependencies jar
    }

    //Create a Spark context
    val sc= new SparkContext(conf)

    //Select the input path
    var readPath=""
    if(Local)
      readPath=ReadPathLocal
    else
      readPath=ReadPathS3


    //Read the user, items and shelves of the dataset from S3
    val user_item_shelves: RDD[(Int, Array[(Int, Array[Int])])] =sc.objectFile(readPath+"/2016-1-14-10-40-15-19users")
    .persist(StorageLevel.MEMORY_AND_DISK_2)

    //Count the number of users
    val users_size = user_item_shelves.count()

    println("users size: "+users_size)
    if(Debug)user_item_shelves.foreach(x=>println(x._1+": "+ x._2.map(y=> "("+y._1+","+y._2.mkString(",")+")").mkString(",")+")"))

    //Split the rdd between train and test set
    val rdd_splitted=user_item_shelves.map {case (u, ib_is)=>
      val size: Int = ib_is.length
      var train: Array[(Int, Array[Int])]=Array()
      var test: Array[(Int, Array[Int])]=Array()
      ib_is.foreach{x=>
        if(scala.util.Random.nextDouble()<TrainSetPercentage){
          train=train:+x
        }else{
          test=test:+x
        }
      }
      (u, (train, test))
    }.persist(StorageLevel.MEMORY_AND_DISK_2)

    //Create list of user, item, reviews and list of shelves and retrieve their unique index
    val user_ff=rdd_splitted.flatMap{case (u, (train, test))=>
      val shelves=train.flatMap(_._2)
      shelves.groupBy(x=>x).map(x=>(x._1, x._2.length))
                    .toArray
                    .map{case (s,ff)=> (s,(u, ff))}
    } //(u, (feature, f. frequency))
    .persist(StorageLevel.MEMORY_AND_DISK)


    if(Debug) println("user_ff")
    if(Debug)user_ff.foreach(x=>println(x._1+": "+x._2))

    //Calculate the user feature frequency in the whole dataset
    val ffw=user_ff.map{case (s,(u, ff))=>(s, u)}
                   .groupByKey()
                   .map{case (s, s_u)=> (s, log10(users_size.toDouble/s_u.toArray.distinct.length.toDouble))}


    //Weight user features
    val user_ffw=user_ff.join(ffw).map { case (s, ((u, ff), fw)) =>
      (u, (s, fw * ff.toDouble))
    }.groupByKey()
    .persist(StorageLevel.MEMORY_AND_DISK)




    //Calculate the cosine similarities between users (u1, (u2, csim(u1,u2)))
    val similarities_pairs=user_ffw.cartesian(user_ffw)
                                .filter{case ((u1,m1),(u2,m2)) => u1!=u2} //Filter if user
                                .map{case (((u1,a1),(u2,a2)))=> (u1,(u2, cosineSimilarityList(a1.toList,a2.toList)))}
                                .filter{case (u1,(u2, sim)) => sim> MinUsersSimilarity}
                                .groupByKey()

    if(Debug)println("similarities_pairs")
    if(Debug)similarities_pairs.foreach(x=>println(x._1+": "+x._2.mkString(",")))

    //Get the N most similar users
    val n_mostSimilarUsers=similarities_pairs.flatMap{case (u, sim_u)=>
      sim_u.toArray
        .sortBy(_._2).map(_._1)
        .reverse
        .take(NSimilarUsers)
        .map(simu => (simu, u))
    }


    if(Debug)println("n_mostSimilarUsers")
    if(Debug)n_mostSimilarUsers.foreach(println)

    //Get the items in the neighborhood (iu, Array[(ib, Iterable(is)]))
    val user_items_join=n_mostSimilarUsers.join(user_item_shelves)

    ///Get the N items that consists of features that are prevalent in the feature profiles of the user neighborhood
    val top_N_items=user_items_join.flatMap{case (siu, (iu, ibis)) => ibis.flatMap{case (ib, ais)=>ais.map(s=>
      (iu, (ib, s))
    )}}
      //Aggegate a feature->items map for retrieve the items frquency in the user neightborhood
      .aggregateByKey(Map[Int, Set[Int]]())((m1, v)=>{
        v match { case (ib, is)=>m1+(is->(m1.getOrElse[Set[Int]](is, Set())+ib))}},
        (m1, m2)=> m1++m2.map{case (k,v)=>k->(m1.getOrElse[Set[Int]](k, Set())++v)}
      )
      .map{case (u, m)=> //Get the feature frquency in the neighborhood
        (u, m.toArray.flatMap{case (k,sib)=>
          sib.map{ib=>(ib, sib.size)}
        }
          )
      }//Take the top N items based on its feature weight
      .map{case (u, ib_nbfw)=>
        (u, ib_nbfw.groupBy(_._1)
          .map{case (k,ibf)=>k->ibf.map(_._2).sum}
          .toArray
          .sortBy(_._2)
          .reverse
          .take(NRecemendedItems))
      }.persist(StorageLevel.MEMORY_AND_DISK)

    if(Debug)println("User reviews")
    if(Debug)top_N_items.foreach(x=>println(x._1+": "+ x._2.mkString(",")))

    //Test set items
    val testItems=rdd_splitted.map{case (ugid, (vtr, vt))=> (ugid, vt.map(y=>y._1))} // (ugid, bgid)
                              .persist(StorageLevel.MEMORY_AND_DISK)
    //Recommended items
    val results=top_N_items.map{case (u, lb_f)=>(u, lb_f.map{case (b, ff)=>b})}
                           .persist(StorageLevel.MEMORY_AND_DISK)

    //Calculate the True Positive items number
    val user_tp=results.join(testItems)  // (u, (Array[b_res], Array[b_test]))
                           .map{case (u,(b_res, b_test)) => (u, b_res.intersect(b_test).length.toDouble)}
                           .persist(StorageLevel.MEMORY_AND_DISK)

    //Calculate the False Negative items number
    val user_fn=user_tp.join(testItems).map{case (u,(tp, b_test))=> (u, b_test.length-tp)}

    //Calculate the False Positive items number
    val user_fp=user_tp.join(results).map{case (uid, (tp, recom))=> (uid, recom.length-tp)}

    //Precision tp/(tp+fp)
    val user_precision=user_tp.join(user_fp).map{case (uid,(tp, fp))=>(uid, tp/(tp+fp))}
                              .persist(StorageLevel.MEMORY_AND_DISK)

    //Recall tp/(tp+fn)
    val user_recall=user_tp.join(user_fn)
                           .map{case (uid,(tp, fn))=>(uid,tp/(tp+fn))}
                           .persist(StorageLevel.MEMORY_AND_DISK)

    //Calculate the F1-score F1-Score = (2*precision*recall) /(Precision+Recall)
    val user_f1_score=user_precision.join(user_recall).map { case (uid, (precision, recall)) =>
      val f1_score = (2.0 * precision * recall) / (precision + recall)
      if (f1_score.isNaN)
        (uid, 0.0)
      else
        (uid, 0.0)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    //Name of the output folder
    val format = new SimpleDateFormat("y-M-d-hh-mm-ss")
    val dateString=format.format(Calendar.getInstance().getTime)
    var ResultfolderName = ""
    if(Local)
      ResultfolderName=ResultfolderName+SavePathLocal+dateString+"-"+users_size+"users-"+NRecemendedItems+"items"+NSimilarUsers+"sim"
    else
      ResultfolderName=ResultfolderName+SavePathS3+dateString+"-"+users_size+"users"+NRecemendedItems+"items"+NSimilarUsers+"sim"

    //Save the measures
    user_precision.coalesce(20, true).saveAsTextFile(ResultfolderName+"/user_precision")
    user_recall.coalesce(20, true).saveAsTextFile(ResultfolderName+"/user_recall")
    user_f1_score.coalesce(20, true).saveAsTextFile(ResultfolderName+"/user_f1_score")

    //Stop Spark context
    sc.stop()
  }

  //Function for calculate the cosine similarity between two vector
  def cosineSimilarityList(a1: List[(Int, Double)], a2: List[(Int, Double)]):Double={
    val common=a1.intersect(a2)
    if(common.isEmpty) {
      0.0
    }else {
      val common_values=common.map(s => (a1.find(_._1 == s._1).get._2,  a2.find(_._1 == s._1).get._2))
      val num = common_values.map(x=>x._1+x._2).sum
      val m1_d = Math.sqrt(common_values.map(x=> Math.pow(x._1, 2)).sum)
      val m2_d = Math.sqrt(common_values.map(x=> Math.pow(x._2, 2)).sum)
      num / (m1_d * m2_d)
    }
  }
}
