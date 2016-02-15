package com.klout.scoozie.utils

import java.io.StringWriter
import java.util.Properties

object PropertyImplicits {
    implicit class MapWrapper(val map: Map[String, String]) extends AnyVal {
        def getAsParameter(key: String): String = "${" + map.get(key).get + "}"
        def toProperties: Properties = {
            val properties = new java.util.Properties
            map.foldLeft(properties){ case (props, (k,v)) =>
                props.put(k,v)
                props
            }
        }
    }

    implicit class PropertyWrapper(val properties: Properties) extends AnyVal {
        def toWritableString = {
            val stringWriter = new StringWriter()
            properties.store(stringWriter, "")

            stringWriter.getBuffer.toString
        }
    }
}
