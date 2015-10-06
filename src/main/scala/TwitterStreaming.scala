import java.io.{InputStream, File}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
 * Created by bagas on 28/09/15.
 */
object TwitterStreaming {

  def main(args: Array[String]) {

    // URL of the Spark cluster
    val sparkUrl = "local[*]"

    // Configure Twitter credentials using twitter.txt
    setTwitterKey()

    // set log to error to reduce verbosity
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


    val conf = new SparkConf().setMaster(sparkUrl).setAppName("TwitterStreaming")
    val ssc = new StreamingContext(conf, Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None)

    // create DStream of retweets only
    val retweets = tweets.filter(status => status.isRetweet == true)

    // transform to map(original tweet, retweet counts)
    val retweetsMap = retweets.map(status => {
      val original_tweet = status.getRetweetedStatus
      val original_tweet_count = original_tweet.getRetweetCount
      (original_tweet, original_tweet_count)
    })

    // create window of length 60 seconds, with sliding interval 1 second
    val retweetWindow = retweetsMap.window(Seconds(60), Seconds(1))

    // group the tweet by key
    // so the dstream has values like this:
    // (tweetA, [10, 12, 14])
    // (tweetB, [15, 18, 20])
    // the value array is the retweet counts over the window
    val retweetWindowGroup = retweetWindow.groupByKey()

    // to get the total number of retweet, subtract the max number
    // of retweet count to the min number of retweet
    // for example the dstream
    // (tweetA, [10, 12, 14])
    // (tweetB, [15, 18, 20])
    // then tweetA has a count of 14-10 = 4 and tweet B 20-15=5
    // the rdd become
    // (tweetA, 4)
    // (tweetB, 5)
    val maxRetweet = retweetWindowGroup.mapValues(value => value.max-value.min)

    // sort the dstream by value descending to get the top retweet. Print only the top 5
    // the map is flipped so the count is for the key
    maxRetweet.map(x => (x._2, x._1.getText)).transform(_.sortBy(_._1, false)).print(5)

    ssc.start()
    ssc.awaitTermination()

  }

  def setTwitterKey(): Unit = {

    val stream : InputStream = getClass.getResourceAsStream("/twitter.txt")

    val lines = Source.fromInputStream( stream ).getLines.map(line => {
      val kvp = line.split("=")
      (kvp(0).trim(), kvp(1).trim())
    } )

    for (line <- lines) {
      val key = "twitter4j.oauth." + line._1
      val value = line._2
      System.setProperty(key, value)
    }

  }

}
