package com.liuser.ecommerce.realline.handler

import java.util
import java.util.Properties

import com.liuser.Ecommerce.common.utils.PropertiesUtil
import com.liuser.ecommerce.realline.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {
    def handle(adsLogDstream: DStream[AdsLog]): Unit ={
        //每日每用户每广告的点击次数
        val clickcountPerAdsPerDayDStream: DStream[(String, Long)] = adsLogDstream.map {
            adsLog => (adsLog.getDate() + "_" + adsLog.userId + "_" + adsLog.adsId, 1L)
            }.reduceByKey(_ + _)

        clickcountPerAdsPerDayDStream.foreachRDD(rdd=> {

            val prop: Properties = PropertiesUtil.load("config.properties")

            // 使用 foreachPartition  减少资源浪费
            rdd.foreachPartition { adsItr =>
                //建立redis连接
                val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt) //driver
                //redis结构  hash  key: day  field: user_ads  value:count
                adsItr.foreach{  case (logkey, count) =>
                    val day_user_ads: Array[String] = logkey.split("_")
                    val day: String = day_user_ads(0)
                    val user: String = day_user_ads(1)
                    val ads: String = day_user_ads(2)
                    jedis.hincrBy(day,user+"_"+ads ,count )  //hincrby 累加
                }
                jedis.close()

            }

        }


        )


    }
        def check(sparkContext: SparkContext, adsLogDstream:DStream[AdsLog])={
            //利用filter过滤   依据是blacklist
            adsLogDstream.transform{
                val prop: Properties = PropertiesUtil.load("config.properties")
                val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)

                val blackListSet: util.Set[String] = jedis.smembers("blacklist")
                val blackListSetBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackListSet)
                rdd=>rdd.filter{adslog =>
                    !blackListSetBC.value.contains(adslog.userId)
                }
            }



        }
}

