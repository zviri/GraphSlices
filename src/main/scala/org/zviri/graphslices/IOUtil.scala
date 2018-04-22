package org.zviri.graphslices

import java.io.{File, PrintWriter}

import play.api.libs.json.{JsValue, Json}

object IOUtil {

   def saveJsonLine(filePath: String, items: Iterable[JsValue]) = {
     var writerOpt: Option[PrintWriter] = None
     try {
       writerOpt = Some(new PrintWriter(new File(filePath)))
       items.foreach(item => writerOpt.get.println(Json.stringify(item)))
     } finally {
       writerOpt match {
        case Some(writer) => writer.close()
        case _ =>
      }
     }
   }

}
