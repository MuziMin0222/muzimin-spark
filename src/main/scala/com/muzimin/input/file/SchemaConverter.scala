package com.muzimin.input.file

import com.muzimin.utils.FileUtils
import org.apache.spark.sql.types._
import play.api.libs.json.{JsArray, JsNull, JsObject, JsPath, JsString, JsValue, Json}

import scala.annotation.tailrec

/**
 * @author: 李煌民
 * @date: 2021-12-15 16:03
 *        ${description}
 **/
object SchemaConverter {
  final lazy val SchemaFieldName = "name"
  final lazy val SchemaFieldType = "type"
  final lazy val SchemaFieldId = "id"
  final lazy val SchemaStructContents = "properties"
  final lazy val SchemaArrayContents = "items"
  final lazy val SchemaRoot = "/"
  final lazy val typeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "object" -> StructType,
    "array" -> ArrayType
  )

  def convert(fileContent: String): StructType = {
    convert(loadSchemaJson(fileContent))
  }

  def convert(inputSchema: JsValue): StructType = {
    //获取json中的name字段的值
    val name = getJsonName(inputSchema)

    val typeName = getJsonType(inputSchema)

    if (name == SchemaRoot && typeName.equals("object")) {
      val properties = (inputSchema \ SchemaStructContents).as[JsObject]
      convertJsonStruct(new StructType, properties, properties.keys.toList)
    } else {
      throw new IllegalArgumentException(s"schema needs root level called <$SchemaRoot> and root type <object>. Current root is <$name> and type is <$typeName>")
    }
  }

  //tailrec用于尾递归优化的注解
  @tailrec
  private def convertJsonStruct(schema: StructType, json: JsObject, jsonKeys: List[String]): StructType = {
    jsonKeys match {
      case Nil => schema
      case head :: tail => {
        val enrichedSchema = addJsonField(schema, (json \ head).as[JsValue])
        convertJsonStruct(enrichedSchema, json, tail)
      }
    }
  }

  private def addJsonField(schema: StructType, json: JsValue): StructType = {
    val fieldType = getJsonType(json)
    val (dataType, nullable) = typeMap(fieldType.typeName) match {

      case dataType: DataType =>
        (dataType, fieldType.nullable)

      case ArrayType =>
        val dataType = ArrayType(getDataType(json, JsPath \ SchemaArrayContents \ SchemaStructContents))
        (dataType, getJsonType(json).nullable)

      case StructType =>
        val dataType = getDataType(json, JsPath \ SchemaStructContents)
        (dataType, getJsonType(json).nullable)
    }

    schema.add(getJsonName(json), dataType, nullable = nullable)
  }

  private def getDataType(json: JsValue, contentPath: JsPath): DataType = {
    val content = contentPath.asSingleJson(json).as[JsObject]
    convertJsonStruct(new StructType, content, content.keys.toList)
  }

  /**
   * 将文件中的内容转为JsValue对象
   *
   * @param fileContent 文件中的内容
   * @return
   */
  def loadSchemaJson(fileContent: String): JsValue = {
    Json.parse(fileContent)
  }

  //获取json字符串中字段名称为name的值
  def getJsonName(json: JsValue): String = {
    (json \ SchemaFieldName).as[String]
  }

  //将schema中的数据转为SchemaType样例类
  def getJsonType(json: JsValue): SchemaType = {
    val id = getJsonId(json)

    (json \ SchemaFieldType).getOrElse(JsNull) match {
      case JsString(value) => {
        SchemaType(value, nullable = false)
      }
      case JsArray(value) if (value.size == 2) => {
        value.find(_ != JsString("null"))
          .map(i => {
            SchemaType(i.as[String], nullable = true)
          })
          .getOrElse(throw new IllegalArgumentException(s"Incorrect definition of a nullable parameter at <$id>"))
      }
      case JsNull => {
        throw new IllegalArgumentException(s"No <$SchemaType> in schema at <$id>")
      }
      case t => {
        throw new IllegalArgumentException(s"Unsupported type <${t.toString}> in schema at <$id>")
      }
    }
  }

  def getJsonId(json: JsValue): String = {
    (json \ SchemaFieldId).as[String]
  }

  def main(args: Array[String]): Unit = {
    val content = FileUtils.readConfigurationFile("/Users/muzimin/study/test_files/a.json")

    println(content)

    val value = loadSchemaJson(content)
    println(value)

    println(getJsonName(value))
  }
}
