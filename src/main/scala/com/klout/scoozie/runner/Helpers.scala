package com.klout.scoozie.runner

/**
 * Created by nathan on 18/02/2016.
 */
object Helpers {

    /*
 * Requires: propertyFile is the path to a property file containing properties formatted as follows:
 *   PropertyName1=PropertyValue1
 *   PropertyName2=PropertyValue2
 *   ...
 */
    def readProperties(propertyFile: String): Map[String, String] = {
        var propertyMap: Map[String, String] = Map.empty
        io.Source.fromFile(propertyFile).getLines.foreach { line =>
            if (!line.isEmpty && line(0) != '#') {
                propertyMap = addProperty(propertyMap, line)
            }
        }
        propertyMap
    }

    def addProperty(propertyMap: Map[String, String], propertyString: String): Map[String, String] = {
        val property: Array[String] = propertyString.split("=")
        if (property.length != 2)
            throw new RuntimeException("error: property file not correctly formatted")
        propertyMap + (property(0) -> property(1))
    }

    def retryable[T](body: () => T): T = {
        val backoff: Double = 1.5
        def retryable0(body: () => T, remaining: Int, retrySleep: Double): T = {
            try {
                body()
            } catch {
                case t: Throwable =>
                    println("ERROR : Unexpected exception ")
                    t.printStackTrace
                    if (remaining > 0) {
                        println("Retries left: " + remaining + ". Sleeping for " + retrySleep)
                        Thread.sleep(retrySleep.toLong)
                        retryable0(body, remaining - 1, retrySleep * backoff)
                    } else
                        throw new RuntimeException("error: Allowed number of retries exceeded. Exiting with failure.")
            }
        }
        retryable0(body, 5, 2000)
    }
}
