package main.scala

import java.util.UUID


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.mllib.linalg.distributed._
import Function.tupled
import scala.math._
import scala.util.Random.nextInt
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.math._
import java.lang._

/**
 * Created by Simo on 11.12.15.
 */
object HybridRecomendation {
  val MinUsersSimilarity = 0.1
  val NSimilarUsers=20
  val NRecemendedItems=100
  val DefaultFeatureValue=1.0
  val MinShelveVotes=10
  val TrainSetPercentage=0.7



  def main(args: Array[String]): Unit = {
    //Spark configuration
    val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", "XX-XX-XX-XX") //Cassandra server configuration
        .set("spark.cassandra.input.fetch.size_in_rows", "10") //Limit fetch size
        //.set("connection.compression", "SNAPPY")
        //.setAppName("Simple Application")
        .setMaster("spark://XX-XX-XX-XX:7077")
        //.setMaster("local[8]")
        .setJars(Seq(System.getProperty("user.dir") + "/my-project-assembly.jar"))


    //Create a Spark context
    val sc= new SparkContext(conf)
    val testfile=sc.textFile("s3n://prs-simone/test-s3.txt")
    println(testfile.count())


    //Read users reviews
    val rdd = sc.cassandraTable("prs", "users")
      .select("id","gid", "name", "list_reviews")
      .where("private = ?", false)

    //Store users size for later use
    val users_size = sc.broadcast(rdd.count())

    //Debug print number of users on the dataset
    println(rdd.count())

    //Tuples of users goodreads id and matrix index
    val user_id_map=rdd.map(x=>x.get[Int]("gid"))
      .distinct()
      .zipWithIndex()
      .cache()

    //Tuple of matrix index and goodreads id
    val user_id_map_reversed=user_id_map.map(_.swap)


    //Items gid asociated to the matrix index
    val items_id_map=rdd.flatMap(x=>x.getList[UDTValue]("list_reviews")
      .toArray
      .map(y=> y.get[UDTValue]("book").get[Int]("gid")))
      .distinct()
      .zipWithIndex()

    //Shelve gid associated with the matrix index
    val shelve_id_map=rdd.flatMap(x=> x.getList[UDTValue]("list_reviews")
      .toArray.flatMap(y=>  y.get[UDTValue]("book")
                             .getList[UDTValue]("list_shelves")
      .toArray
      .filter(z=>(z.get[Int]("count")>10 &&
        !z.get[String]("shelve").contains("to") &&
        !z.get[String]("shelve").contains("read") &&
        !z.get[String]("shelve").contains("own") &&
        !z.get[String]("shelve").contains("my-books")))
      .map(z=> z.get[String]("shelve"))))
      .distinct()
      .zipWithIndex().cache()


    //Split the rdd between train and test set
    val rdd_splitted=rdd.map { (x: CassandraRow) =>
      val size: Int = x.getList[UDTValue]("list_reviews").size
      val train_index: Int = (size * TrainSetPercentage).asInstanceOf[Int]
      val train = x.getList[UDTValue]("list_reviews").slice(0, train_index)
      val test = x.getList[UDTValue]("list_reviews").slice(train_index, size)
      (x.get[Int]("gid"), (train, test))
    }.cache()
    //RDD(Int, (Vector(UDT), Vector(-udt)))) (ugid, (vector(train), Vector(test)))
    rdd.unpersist()



    //Create list of user, item, reviews and list of shelves and retrieve their unique index
    val user_items_pairs1=rdd_splitted.flatMap(x=>
      x._2._1 //Trainset
      .toArray
      .map(y=>    (x._1,
                  (y.get[UDTValue]("book").get[Int]("gid"),
                  (1.0, y.get[UDTValue]("book")
                  .getList[UDTValue]("list_shelves")
                  .toArray
                  .filter(z=>( z.get[Int]("count")>MinShelveVotes &&
                      !z.get[String]("shelve").contains("to") &&
                      !z.get[String]("shelve").contains("read") &&
                      !z.get[String]("shelve").contains("own") &&
                      z.get[String]("shelve")!="my-books"))
                  .map(x=>x.get[String]("shelve")
                  ))))))
      //RDD[Int, (Int,(Double, Array(String)))] (u,(b,(r,ls)))

    //Retrieve the index of avery items and user
    val user_items_with_user_index=user_items_pairs1.join(user_id_map).map{case (u, ((b, (r, ls)), iu)) => (b,(iu, (u, (r,ls))))}
    val user_items_with_both_index=user_items_with_user_index.join(items_id_map).cache() //(b,((iu,(u,(r,ls))),ib))

    //Create user item matrix
    val user_item_matrix=new CoordinateMatrix(user_items_with_both_index
      .map{case (b,((iu,(u,(r,ls))),ib)) => new MatrixEntry(iu, ib, r)})
      .toBlockMatrix()
      .cache()



    //Retrived the index of shelves
    val item_shelve=user_items_with_both_index.flatMap{case (b,((iu,(u,(r,ls))),ib))=>ls.map((x:String) => (x, (ib, iu)))}
    val item_shelve_with_index=item_shelve.join(shelve_id_map)

    //Create item shelves matrix
    val item_shelve_matrix=new CoordinateMatrix(item_shelve_with_index.map{case (s,((ib,iu),is)) => (ib,is)}
      .distinct()
      .map{case (ib,is) =>new MatrixEntry(ib, is, DefaultFeatureValue)})
      .toBlockMatrix()
      .cache()

    //Debug: Print matrices dimensions
    println(user_item_matrix.numCols())
    println(user_item_matrix.numRows())
    println(item_shelve_matrix.numRows())
    println(item_shelve_matrix.numCols())


    //Debug vectors
    //val v1=Vectors.dense(Array(0.60, 0.24, 0.30, 0))
    //val v2=Vectors.dense(Array(0.30, 0.24, 0, 0))

    //Function for calculate the cosine similarity between two vector
    def cosineSimilarity(v1: Vector, v2: Vector):Double={
      val v1_indexes: Array[Int]=v1.toSparse.indices
      val v2_indexes: Array[Int]=v2.toSparse.indices
      val indexes=v1_indexes.intersect(v2_indexes)
      if(indexes.length ==0) {
        0.0
      }else {
        val num = indexes.map(i => v1.apply(i) * v2.apply(i)).sum
        val v1_d = Math.sqrt(indexes.map(i => Math.pow(v1.apply(i), 2)).sum)
        val v2_d = Math.sqrt(indexes.map(i => Math.pow(v2.apply(i), 2)).sum)
        num / (v1_d * v2_d)
      }
    }

    //Calculate user feature matrix
    val user_feature_matrix=user_item_matrix.multiply(item_shelve_matrix)
                                            .cache()

    //Feature weighting
    val feature_users_indexed_matrix=user_feature_matrix.transpose.toIndexedRowMatrix()
    val feature_users_rows=feature_users_indexed_matrix.rows
    val feature_weight=feature_users_rows.map{case IndexedRow(is, v)=> (is, log10(users_size.value/v.numActives.toDouble))}
                                         .collectAsMap()

    //User and Vector of the weighted features
    val user_feature_paris=user_feature_matrix.toIndexedRowMatrix().rows.map{case IndexedRow(iu, v)=> (iu,
      Vectors.sparse(v.size, v.toSparse.indices, v.toSparse.indices.map(
        i=> v.apply(i) * feature_weight(i))
      ))} // (iu, Vector(features_normalized))
      .join(user_id_map_reversed)
      .map{case (iu, (fw, u)) => (u, fw)}
      .cache()

    //Calculate the cosine similarities between users (u1, (u2, csim(u1,u2)))
    val similarities_pairs=user_feature_paris.cartesian(user_feature_paris).filter{case ((u1,v1),(u2,v2)) => u1!=u2  }
      .map{case (((u1,v1),(u2,v2)))=> (u1,(u2, cosineSimilarity(v1,v2)))}
      .filter{case (u1,(u2, sim)) => sim> MinUsersSimilarity} //TODO adjust the minimum similarity threashold
      .groupByKey

    //Get the 20 similar users
    val n_mostSimilarUsers=similarities_pairs.flatMap{case (u, sim)=>
      sim.toArray
        .sortBy(_._2).map(_._1)
        .reverse
        .take(NSimilarUsers)
        .map(simu => (simu, u))
    }



    //Ge the gid of user and items
    val user_items_with_gid=user_items_pairs1.flatMap{case (u,(b, (r, ls)))=>
      ls.map((s:String) => (u, (b, s)))}
      .groupByKey


    //Get the items in the neighborhood (iu, Array[(ib, Iterable(is)]))
    val user_items=n_mostSimilarUsers.join(user_items_with_gid) //user_items_pairs)
        .map{case (siu, (iu, ibis)) => (iu, ibis)}
        .groupByKey //RDD[(Long, Iterable[(Long, Long)])]
        .map{case (iu, ibis)=>  (iu, ibis.flatten)}//RDD[(iu, Iterable[(ib, is)])]
        .cache()

    //Get the feature frequency in the neighborhood (u,(Array[(s, ff)]))
    val item_feature_frequency=user_items.map{
      case (iu, ibis) => (iu, ibis.groupBy( _._2 ).mapValues(_.size).toArray)//RDD[(iu, Array[(is, Iterable(numIsInThe neightbor of iu))])]
    }

    //Join the feature frequency to the items frequency
    val user_items_FF=user_items.join(item_feature_frequency)//RDD[(Long, (Iterable[(Long, Long)], Array[(Long, Int)]))]
      .map{
        case (iu, (ibis, listFF))=> (iu,
          ibis.map{
            case (ib, is)=> (ib,( is, listFF.find(_._1 == is).get._2))
          })
      }//RDD[(Long, Iterable[(Long, (Long, Int))])]

    //Get the N items that consists of features that are prevalent in the feature profiles of the user neighbors
    //RDD[(iu, Array[(ib, sumff)])]
    val top_N_items=user_items_FF.map{
      case (iu, ib_is_ff)=> (iu,  ib_is_ff.groupBy( _._1 )
          .mapValues(_.map(_._2._2).sum)
          .toArray
          .sortBy(_._2)
          .reverse
          .take(NRecemendedItems) //Adjust the number of items to reccomend
        )
    }

    //Test set items
    val testItems=rdd_splitted.map{case (ugid, (vtr, vt))=> (ugid, vt.map(y=>y.get[UDTValue]("book").get[Int]("gid")).toArray)} // (ugid, bgid)

    //Recommended items
    val results=top_N_items.map{case (u, lb_f)=>(u, lb_f.map{case (b, ff)=>b})}

    //Accuracy of the prediction by users
    val accuracy_rate=results.join(testItems)  // (u, (Array[b_res], Array[b_test]))
                             .map{case (u,(b_res, b_test)) => (u, b_res.intersect(b_test).size.toDouble / b_test.size.toDouble * 100.0) }


    //val top_N_with_id=top_N_items.join(user_id_map.map(_.swap)).map{case (iu, (ib_w, u)=> (iu, (ib_w.join(items_id_map.map(._swap)), u)))} //RDD[Long, (Array(Long, Int), Int)]
    //top_N_items.collect().foreach{ x => (print(x._1+": "), x._2.foreach((y:(Int, Int)) =>print(" ("+y._1+","+y._2+")")), println())}

    //Save recommendation
    val format = new SimpleDateFormat("y-M-d-hh-mm-ss")
    val dateString=format.format(Calendar.getInstance().getTime())
    val myRDD = top_N_items.map{case (u, lb_f)=>u+", "+ lb_f.mkString(",") }
                            .saveAsTextFile("s3n://prs-simone/"+dateString+"/train")
    rdd_splitted.map{case (ugid, (vtr, vt))=> ugid+", "+vt.map(y=>y.get[UDTValue]("book").get[Int]("gid"))
                                                          .toArray
                                                          .mkString(",")}
                .saveAsTextFile("s3n://prs-simone/"+dateString+"/test")


    //Save accuracy rate, mean, max
    accuracy_rate.saveAsTextFile("s3n://prs-simone/"+dateString+"/accuracy_rate")
    val acc_rate=accuracy_rate.map(_._2).collect()

    sc.parallelize( List(acc_rate.sum / acc_rate.size.toDouble, acc_rate.max )).saveAsTextFile("s3n://prs-simone/"+dateString+"/mean_accuracy_rate")

    sc.stop()



    //Save to cassandra the results
    /*case class Recommendation(id: UUID, user_gid: Int, item_recommended: scala.collection.immutable.Vector[Int])
    val collection = sc.parallelize(Seq(Recommendation( UUID.randomUUID(), 23, Vector(1,2,3)),
                                        Recommendation( UUID.randomUUID(), 24, Vector(2,3,6))))



    collection.saveToCassandra("prs", "recommendation", SomeColumns("id", "user_gid", "item_recommended"))
    */
  }
}
