package main.scala

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * This class perform the Trust Walkers algorithm by using the the Epinions.com algorithm retrieved from http://liris.cnrs.fr/red/
 * Created by Simone Cogno on 28.12.15.
 */
object TrustWalkers_v1 {

  //Constants 
  val NumberRW: Int=1
  val Ksteps: Int=1
  var NumberTestItems=2
  val SavePathS3 = "s3n://prs-simone/results/"
  val ReadPathLocal = "/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/epinions_netowork/"
  val ReadPathS3 = "s3n://prs-simone/input/"
  val SavePathLocal = "/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/Prototype1/results/"
  val debug = false
  var local = false


  def main(args: Array[String]): Unit = {

    //Program args (local, number of test items)
    args match {
      case Array() =>
      case Array(l) => local = l.toBoolean
      case Array(l, ntestitems) => {
        local = l.toBoolean
        NumberTestItems=ntestitems.toInt
      }
    }

    //Spark configuration
    val conf = new SparkConf(true)
    if (local) {
      conf.setAppName("Hybrid Algorithm")
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

    //Read epinions dataset 300 users, 50'000 reviews and 40'000 items
    //[1]iduser,[2]idtrusted, [4]idproduct,[3]review_rating
    val epinions_text = sc.textFile(readPath+"user_trusted_reviews.txt")
    val epinions = epinions_text.map(x => x.split(","))
                                .cache()

    //User-reviews list of epinions.com dataset
    //[0]iduser,[1]idtrusted, [2]idproduct,[3]rating
    val all_reviews=epinions.map(x=>((x.apply(1).toLong,x.apply(2).toInt),x.apply(3).toDouble))
      .reduceByKey((a,b)=>a)
      .map{case ((uid, pid), r)=>(uid, (pid, r))}
      .groupByKey()
      .setName("users_reviews")
      .cache()

    //Construct the edge from the dataset
    val  user_distinctt= epinions.map(x => (x.apply(0).toLong,x.apply(1).toLong))
      .distinct()
      .groupByKey()
    val  test= epinions.map(x => (x.apply(0).toLong,x.apply(1).toLong))
      .distinct()
      .groupByKey()
      .flatMap{case (uid, utrustedList)=>
          utrustedList.take(10)
      }
    val id_trusted =user_distinctt.flatMap{case (uid, utrustedList)=>
          utrustedList.take(10).map(trusted=>(uid, trusted))
      }

    //Construct the RDD containing the edge of the graph
    val edges=id_trusted.map{case (uid, trusted)=>Edge(uid, trusted, "")}

    //Get the users reviews of trusted items
    val users_reviews=id_trusted.flatMap{case (uid, trusted)=>Array(uid, trusted)}
                               .distinct()
                               .map(x=>(x, 1))
                               .join(all_reviews)
                               .map{case (uid, (_, reviews))=>(uid, reviews)}

    //Print for debug
    users_reviews.foreach{case (uid, pid_r)=>println(uid+": "+pid_r.size)}

    //Split users reviews in the train and the test set
    val user_reviews_splitted=users_reviews.map { case (uid, reviews) =>
      val size: Int = reviews.size
      var train: Iterable[(Int, Double)]=List()
      var test: Iterable[(Int, Double)]=List()
      if(size>NumberTestItems){
        train= reviews
        test= reviews.slice(0, NumberTestItems)
      }else{
        train=reviews
        test=reviews
      }
      (uid, (train, test))
    }.setName("user_reviews_splitted")
      .cache()
    user_reviews_splitted.foreach{case (uid, (train, test))=>println(uid+": "+train.size+" , "+test.size)}

    val users_count=user_reviews_splitted.count()
    println("user count"+users_count)


    //Calculate the Jaccard similarity between the items
    val items_simlarity=user_reviews_splitted.flatMap{case (uid, (train, test))=> test.flatMap{x=> train.map{y=>((x._1,y._1), 1)}}}
                                         .reduceByKey(_+_)
                                         .map{case ((i1, i2), count)=>((i1, i2), count.toDouble/users_count.toDouble)}

    println("item_item_usercount"+items_simlarity.count())

    //Initialize vertices properties in the last array we store (k, the source node id, estimated rating)
    val train_set_users=user_reviews_splitted.map{case (uid, (train, test)) => (uid, (train, test.map{case (pid, r)=>(pid, 0.0)}, Array[(Int, Long, Double)]((-1, uid, -1))))}
    val test_set_users=user_reviews_splitted.map{case (uid, (train, test)) => (uid, test)}

    //Initialize the initial graph with the vertices and edges data
    var initialGraph = Graph(train_set_users, edges).cache()

    //Iterate over recommended items
    for(i <- 0 to (NumberTestItems-1)) {

      //Perform N RWs on the social netowrk graph
      for (rw <- 1 to NumberRW) {

        for (k <- 0 to Ksteps) {
          println("k: "+k.toString)
          //TODO if no neightbors
          //Get neightbors ids
          val sid_nbid_filtered=initialGraph.collectNeighborIds(EdgeDirection.Out).flatMap{case (sid, nbsid)=>nbsid.map(nb_id=>
              (sid, nb_id)).filter{case (sid, nb_id)=>sid!=nb_id}//Delete neighbors equal to the source neighbors or to the random walk source
          }

          //Retrieve the source vertex properties and the neighbors property
          val neighbors_with_vertex_property = sid_nbid_filtered.join(initialGraph.vertices).map{case (sid, (nb_id, sProp))=>
            (nb_id, (sid, sProp))
          }.join(initialGraph.vertices)
          .setName("neighbors_with_vertex_property")
          .cache()

          //Calculate the probability to stay at the neighbor node
          //Give the probability of every neightbor to take their estimated rating
          val max_probability_nb_s_pairs=neighbors_with_vertex_property.flatMap{case (nb_id, ((sid, (_,sRecom,_)), (nbRating,_,_)))=>
            nbRating.map{case (nbpid, nbr)=>((sRecom.toList.apply(i)._1,nbpid), (nb_id, nbr, sid))}
          }.join(items_simlarity)
           .map{case (key,((nb_id, nbr, sid), sim))=>((nb_id, sid), (sim, nbr))}
           .reduceByKey((v1, v2)=>{
             val(sim1, nbr1)=v1
             val(sim2, nbr2)=v2
             if(sim1>sim2)
               (sim1, nbr1)
             else
               (sim2, nbr2)
            }
           ).map{case ((nb_id, sid), (sim, nbr))=>( sid, (nb_id, sim * fK(k), nbr))}//sim * fK calulcate the probability to stay at node at level k
            .setName("max_probability_nb_s_pairs")
            .cache()

           //Perform a random jump on one of the neighbors
           val random_nb_value=max_probability_nb_s_pairs.groupByKey()
                                     .join(initialGraph.vertices)
                                     .flatMap{case (sid, (nb_prop_nbr, (_,_,srwList)))=>
                                       //Perform a random walks for all previus RW of the source node
                                        if(k==0){//First step k=0
                                          //Get  random neightbor
                                          val(nb_id, nbprob, nbr)=nb_prop_nbr.toList.apply(scala.util.Random.nextInt(nb_prop_nbr.size))
                                          //Take the rating of the random Nb only for a probability nbsim or if it is the last step
                                          if (scala.util.Random.nextDouble()<nbprob)//Probability to stop the RW at this node
                                            Array((nb_id, (k, sid, nbr)))//Save the first estimated rating value
                                          else
                                            Array((nb_id, (k, sid, -1.0)))//We don't stay at this node, so we leave the value to -1
                                        }else{//All the next steps k>0
                                          srwList.filter(x=>x._1==k-1)//Get only the last (k-1) RW entry
                                                 .filter(x=>x._3<0)//Check if the source RW is already finished (not -1.0), if there is don't take it
                                                 .map{case (_, s, _)=>
                                                   val filtered_nb=nb_prop_nbr.filter{case (nbid,_,_) => nbid != s}//Filter the neightboars if they are the same of the source
                                                   val(nb_id, nbprob, nbr)=filtered_nb.toList.apply(scala.util.Random.nextInt(filtered_nb.size))//Get  random neightbor
                                                   //Take the rating of the random Nb only for a probability nbsim or if it is the last step
                                                   if (scala.util.Random.nextDouble()<nbprob || k==Ksteps)
                                                     (nb_id, (k,sid, nbr))//Add the estimated rating  in the steps list
                                                   else
                                                     (nb_id, (k, sid, -1.0))//Don't take estimated value so put an -1 in the steps list
                                                 }
                                        }
                                     }
          
          initialGraph=initialGraph.outerJoinVertices(random_nb_value.groupByKey().map{case (nbid, rwList)=>(nbid, rwList.toArray)})(
             (vidx, VD, U)=>(VD._1, VD._2, VD._3 ++ U.getOrElse[Array[(Int, Long, Double)]](Array[(Int, Long, Double)]())) 
          ) //Update the vertex values

        }

        //Extract the RW results for item i to user s
        val rwResults=initialGraph.vertices.flatMap{case (vidx,(vrw, vrecom, rwList))=>
                                              rwList.filter{case (nb, s, rwr)=> rwr > 0}
                                            }
                                           .map{case (_, s, rwr)=>(s,rwr)}
        //Update rating of vertices item i
        initialGraph=initialGraph.outerJoinVertices(rwResults){(vidx, VD, U)=>
          val (pid,r)=VD._2.toList.apply(i)
          val list_updated=VD._2.toList.updated( i, (pid,r+U.getOrElse[Double](10000.0)) )
          (VD._1, list_updated, VD._3)
        }
      }

      //Calculate the average of steps by dividing vertices to number of RWs
      initialGraph=initialGraph.mapVertices[(Iterable[(Int, Double)], Iterable[(Int, Double)], Array[(Int, Long, Double)])]{
        case (vidx,(rw, rcom, _))=>{
          val (pid,rwSum)=rcom.toList.apply(i)
          val rw_avg={
            if(rwSum>100){
              val er = rwSum%100
              val numNullRWs=(rwSum - (rwSum%100)).toInt/10000
              (pid, er/(NumberRW-numNullRWs).toDouble)
            }else{
              (pid, rwSum/NumberRW.toDouble)
            }
          }
          val list_updated=rcom.toList.updated(i, rw_avg)
          (rw, list_updated, Array())
        }
      }
    }

    //TODO: Compare items recommended with the test set of rating items
    //Calculate the difference between the estimated and the real rating
    val rmse=initialGraph.vertices.join(test_set_users)  // (u, (Array[b_res], Array[b_test]))
                               .map{case (u,((train, recom,_), b_test)) =>
                                    val sum_of_differences=(recom ++ b_test).groupBy(_._1)
                                                         .map{case (pid, item_ratings)=>
                                                                      if(item_ratings.size!=2)
                                                                        throw new Exception("Seems the number of element is wrong. Size="+item_ratings.size)
                                                                      else{
                                                                          val v1=item_ratings.toArray.apply(0)._2
                                                                          val v2=item_ratings.toArray.apply(1)._2
                                                                          Math.abs(v1-v2) //Difference between the estimated and the real rating
                                                                      }
                                                         }.sum
                                    (u, Math.sqrt(sum_of_differences/b_test.size.toDouble))
                               }

    //Name of the output folder
    val format = new SimpleDateFormat("y-M-d-hh-mm-ss")
    val dateString = format.format(Calendar.getInstance().getTime)
    var ResultfolderName = ""
    if (local)
      ResultfolderName = ResultfolderName + SavePathLocal + dateString + "-TrustWalkers"
    else
      ResultfolderName = ResultfolderName + SavePathS3 + dateString + "-TrustWalkers"

    rmse.coalesce(1, false).saveAsTextFile(ResultfolderName + "rmse")
  }
  //Function to retrieve the probability to stay at node in step k
  def fK(k:Int):Double={
    1.0/(1.0 + Math.pow(Math.E,(-(k/2.0))))
  }

}
