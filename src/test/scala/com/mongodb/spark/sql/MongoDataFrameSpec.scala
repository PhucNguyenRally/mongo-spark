/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.spark.sql

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, MapType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.bson._
import org.bson.types.{Decimal128, ObjectId}
import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql.types.BsonCompatibility
import org.apache.spark.SparkException
import org.scalatest.prop.TableDrivenPropertyChecks
import com.mongodb.spark.sql.helpers.StructFields
import com.mongodb.MongoClient
import com.mongodb.spark.sql.MapFunctions.documentToRow
import org.apache.spark.rdd.RDD

class MongoDataFrameSpec extends RequiresMongoDB with TableDrivenPropertyChecks {
  // scalastyle:off magic.number

  val characters =
    """
     | {"name": "Bilbo Baggins", "age": 50}
     | {"name": "Gandalf", "age": 1000}
     | {"name": "Thorin", "age": 195}
     | {"name": "Balin", "age": 178}
     | {"name": "Kíli", "age": 77}
     | {"name": "Dwalin", "age": 169}
     | {"name": "Óin", "age": 167}
     | {"name": "Glóin", "age": 158}
     | {"name": "Fíli", "age": 82}
     | {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq.map(Document.parse)

  lazy val sparkSession = SparkSession.builder().getOrCreate()

  lazy val readConfigs = Table("readConfig", ReadConfig(sparkSession),
    ReadConfig(sparkSession).withOption(ReadConfig.pipelineIncludeNullFiltersProperty, "false"),
    ReadConfig(sparkSession).withOption(ReadConfig.pipelineIncludeFiltersAndProjectionsProperty, "false"))

  def splitRddOnSchemaValidationLocal(
    bsonDocumentRDD: RDD[BsonDocument],
    schema:          StructType,
    schemaValidator: (BsonDocument, StructType, Array[String]) => Row
  ): (RDD[Row], RDD[BsonDocument]) = {
    var row: Row = Row()
    val validRDD = bsonDocumentRDD.filter(bdoc => {
      try {
        row = schemaValidator(bdoc, schema, Array())
        true
      } catch {
        case ex: Throwable => {
          false
        }
      }
    }).map(doc => row)

    val invalidRDD = bsonDocumentRDD.filter(bdoc => {
      try {
        schemaValidator(bdoc, schema, Array())
        false
      } catch {
        case ex: Throwable => {
          true
        }
      }
    })

    (validRDD, invalidRDD)
  }

  "DataFrameReader" should "should be easily created from the SQLContext and load from Mongo" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val df = SparkSession.builder().getOrCreate().read.mongo()

    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "handle decimals with scales greater than the precision" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    val data =
      """
        |{"_id":"1", "a": {"$numberDecimal":"0.00"}},
        |{"_id":"2", "a": {"$numberDecimal":"0E-14"}}
      """.trim.stripMargin.split("[\\r\\n]+").toSeq.map(Document.parse)

    sc.parallelize(data).saveToMongoDB()

    val df = SparkSession.builder().getOrCreate().read.mongo()
    df.count() should equal(2)
  }

  it should "handle selecting out of order columns" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()

    forAll(readConfigs) { readConfig: ReadConfig =>
      val df = sparkSession.read.mongo(readConfig)
      df.select("name", "age").orderBy("age").rdd.map(r => (r.get(0), r.get(1))).collect() should
        equal(characters.sortBy(_.getInteger("age", 0)).map(doc => (doc.getString("name"), doc.getInteger("age"))))
    }
  }

  it should "handle mixed numerics with long precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedLong).saveToMongoDB()
    val expectedData = List(1L, 1L, 1L)
    val df = SparkSession.builder().getOrCreate().read.mongo().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle mixed numerics with double precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedDouble).saveToMongoDB()
    val expectedData = List(1.0, 1.0, 1.0)
    val df = SparkSession.builder().getOrCreate().read.mongo().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle array fields with null values" in withSparkContext() { sc =>
    sc.parallelize(arrayFieldWithNulls).saveToMongoDB()

    val saveToCollectionName = s"${collectionName}_new"
    val readConf = readConfig.withOptions(Map("collection" -> saveToCollectionName))

    SparkSession.builder().getOrCreate().read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD().collect().toList should equal(arrayFieldWithNulls)
  }

  it should "handle document fields with null values" in withSparkContext() { sc =>
    sc.parallelize(documentFieldWithNulls).saveToMongoDB()

    val saveToCollectionName = s"${collectionName}_new"
    val readConf = readConfig.withOptions(Map("collection" -> saveToCollectionName))

    SparkSession.builder().getOrCreate().read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD().collect().toList should equal(documentFieldWithNulls)
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()

    val sparkSession = SparkSession.builder().getOrCreate()
    forAll(readConfigs) { readConfig: ReadConfig =>
      val df = sparkSession.read.mongo[Character](readConfig)
      val reflectedSchema: StructType = ScalaReflection.schemaFor[Character].dataType.asInstanceOf[StructType]
      val expectedCount = if (!readConfig.pipelineIncludeNullFilters || !readConfig.pipelineIncludeFiltersAndProjections) 10 else 9

      df.schema should equal(reflectedSchema)
      df.count() should equal(expectedCount)
      df.filter("age > 100").count() should equal(6)
    }
  }

  it should "include user pipelines when inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()
    val sparkSession = SparkSession.builder().getOrCreate()

    forAll(readConfigs) { readConfig: ReadConfig =>
      var df = sparkSession.read.option("pipeline", "[{ $match: { name: { $exists: true } } }]").mongo(readConfig)
      df.schema should equal(expectedSchema)
      df.count() should equal(10)
      df.filter("age > 100").count() should equal(6)

      df = sparkSession.read.option("pipeline", "[{ $project: { _id: 1, age: 1 } }]").mongo(readConfig)
      df.schema should equal(createStructType(expectedSchema.fields.filter(p => p.name != "name")))
    }
  }

  it should "use any pipelines when set via the MongoRDD" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()
    val sparkSession = SparkSession.builder().getOrCreate()

    forAll(readConfigs) { readConfig: ReadConfig =>
      val df = MongoSpark.load(sc, readConfig).withPipeline(Seq(Document.parse("{ $match: { name: { $exists: true } } }"))).toDF()
      df.schema should equal(expectedSchema)
      df.count() should equal(10)
      df.filter("age > 100").count() should equal(6)
    }
  }

  it should "throw an exception if pipeline is invalid" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()

    an[IllegalArgumentException] should be thrownBy SparkSession.builder().getOrCreate().read.option("pipeline", "[1, 2, 3]").mongo()
  }

  "DataFrameWriter" should "be easily created from a DataFrame and save to Mongo" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo()

    sparkSession.read.mongo[Character]().count() should equal(9)
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val saveToCollectionName = s"${collectionName}_new"
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("collection" -> saveToCollectionName))

    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo(writeConfig)

    sparkSession.read.option("collection", saveToCollectionName).mongo[Character]().count() should equal(9)
  }

  it should "support INSERT INTO SELECT statements" in withSparkSession() { sparkSession =>
    val df = sparkSession.read.mongo[Character]()

    df.createOrReplaceTempView("people")
    sparkSession.sql("INSERT INTO table people SELECT 'Mort', 1000")

    sparkSession.read.mongo().count() should equal(1)
  }

  it should "support INSERT OVERWRITE SELECT statements" in withSparkSession() { sparkSession =>
    sparkSession.sparkContext.parallelize(characters).saveToMongoDB()
    val df = sparkSession.read.mongo[Character]()

    df.createOrReplaceTempView("people")
    sparkSession.sql("INSERT OVERWRITE table people SELECT 'Mort', 1000")

    sparkSession.read.mongo().count() should equal(1)
  }

  "DataFrames" should "round trip all bson types" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val newCollectionName = s"${collectionName}_new"
    SparkSession.builder().getOrCreate().read.mongo().write.option("collection", newCollectionName).mongo()

    val original = database.getCollection(collectionName).find().iterator().asScala.toList
    val copied = database.getCollection(newCollectionName).find().iterator().asScala.toList
    copied should equal(original)
  }

  it should "be able to cast all types to a string value" in withSparkContext() { sc =>
    if (!serverAtLeast(3, 4)) cancel("MongoDB < 3.4")
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)
    val bsonValuesAsStrings = sc.loadFromMongoDB().toDS[BsonValuesAsStringClass]().first()

    val expected = BsonValuesAsStringClass(
      nullValue = null, // scalastyle:ignore
      int32 = "42",
      int64 = "52",
      bool = "true",
      date = """{ "$date" : 1463497097 }""",
      dbl = "62.0",
      decimal = """{ "$numberDecimal" : "72.01" }""",
      string = "spark connector",
      minKey = """{ "$minKey" : 1 }""",
      maxKey = """{ "$maxKey" : 1 }""",
      objectId = "000000000000000000000000",
      code = """{ "$code" : "int i = 0;" }""",
      codeWithScope = """{ "$code" : "int x = y", "$scope" : { "y" : 1 } }""",
      regex = """{ "$regex" : "^test.*regex.*xyz$", "$options" : "i" }""",
      symbol = """{ "$symbol" : "ruby stuff" }""",
      timestamp = """{ "$timestamp" : { "t" : 305419896, "i" : 5 } }""",
      undefined = """{ "$undefined" : true }""",
      binary = """{ "$binary" : "BQQDAgE=", "$type" : "00" }""",
      oldBinary = """{ "$binary" : "AQEBAQE=", "$type" : "02" }""",
      arrayInt = "[1, 2, 3]",
      document = """{ "a" : 1 }""",
      dbPointer = """{ "$dbPointer" : { "$ref" : "db.coll", "$id" : { "$oid" : "000000000000000000000000" } } }"""
    )
    bsonValuesAsStrings should equal(expected)
  }

  it should "throw an exception when retrieving multi-types recs with inva;lid row " in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()

    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test3").drop()

    myDb.getCollection("test3").insertMany(multiTypesDocs.map(doc => Document.parse(doc)).toList.asJava)

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test3"), Some(ReadConfig(sc)))

    val df = MongoSpark.builder().sparkSession(sparkSession).readConfig(readConfigMap).build().toDF(multiTypesSchema)

    df.createOrReplaceTempView("test3")

    val dfQuery = sparkSession.sql("select * from test3")

    an[SparkException] should be thrownBy dfQuery.show()
  }

  it should "be able to split a RDD into two RDDs based on custom schema validation" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()

    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test3").drop()

    myDb.getCollection("test3").insertMany(multiTypesDocs.map(doc => Document.parse(doc)).toList.asJava)

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test3"), Some(ReadConfig(sc)))

    val (validRDD, invalidRDD) = MongoSpark.builder().sparkSession(sparkSession).readConfig(readConfigMap).build().splitRddOnSchemaValidation(multiTypesSchema, documentToRow)

    validRDD.count() should equal(3)
    invalidRDD.count() should equal(2)

    invalidRDD.foreach(println)

    val df = sparkSession.createDataFrame(validRDD, multiTypesSchema)

    df.createOrReplaceTempView("test3")

    val dfQuery = sparkSession.sql("select * from test3")
    val testDir = "/tmp/test3"

    import java.io.File
    import scala.reflect.io.Directory

    val directory = new Directory(new File(testDir))
    directory.deleteRecursively()

    dfQuery.show()
    dfQuery.write.json(testDir)

  }

  it should "be able to split a RDD into two RDDs using local splitRddOnSchemaValidationLocal" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()

    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test3").drop()

    myDb.getCollection("test3").insertMany(multiTypesDocs.map(doc => Document.parse(doc)).toList.asJava)

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test3"), Some(ReadConfig(sc)))

    val rdd = MongoSpark.builder()
      .sparkSession(sparkSession)
      .readConfig(readConfigMap)
      .build()
      .toRDD[BsonDocument]()

    val (validRDD, invalidRDD) = splitRddOnSchemaValidationLocal(rdd, multiTypesSchema, documentToRow)

    validRDD.count() should equal(3)
    invalidRDD.count() should equal(2)

    invalidRDD.foreach(println)

    val df = sparkSession.createDataFrame(validRDD, multiTypesSchema)

    df.createOrReplaceTempView("test3")

    val dfQuery = sparkSession.sql("select * from test3")
    val testDir = "/tmp/test3"

    import java.io.File
    import scala.reflect.io.Directory

    val directory = new Directory(new File(testDir))
    directory.deleteRecursively()

    dfQuery.show()
    dfQuery.write.json(testDir)

  }

  it should "be able to round trip schemas containing MapTypes" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    val characterMap = characters.map(doc => Row(doc.getString("name"), Map("book" -> "The Hobbit", "author" -> "J. R. R. Tolkien")))
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true)
    ))
    val df = sparkSession.createDataFrame(sc.parallelize(characterMap), schema)
    df.write.mongo()

    val savedDF = sparkSession.read.schema(schema).mongo()
    savedDF.collectAsList() should equal(df.collectAsList())
  }

  it should "be able to upsert and replace data in an existing collection" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val originalData = Seq(SomeData(1, 100), SomeData(2, 200), SomeData(3, 300)).toDS()

    originalData.saveToMongoDB()
    MongoSpark.load(sc).toDS[SomeData]().collect() should contain theSameElementsAs originalData.collect()

    val replacementData = Seq(SomeData(1, 1000), SomeData(2, 2000), SomeData(3, 3000)).toDS()

    replacementData.toDF().write.mode("append").mongo()
    MongoSpark.load(sc).toDS[SomeData]().collect() should contain theSameElementsAs replacementData.collect()
  }

  it should "fail when force insert is set to true and data already exists" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val originalData = Seq(SomeData(1, 100), SomeData(2, 200), SomeData(3, 300)).toDS()
    originalData.saveToMongoDB()

    an[SparkException] should be thrownBy {
      originalData.toDF().write.mode("append").option(WriteConfig.forceInsertProperty, "true").mongo()
    }
  }

  it should "be able to handle optional _id fields when upserting / replacing data in a collection" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val originalData = characters.map(doc => CharacterWithOid(None, doc.getString("name"),
      if (doc.containsKey("age")) Some(doc.getInteger("age")) else None))

    originalData.toDS().saveToMongoDB()
    val savedData = sparkSession.read.mongo[CharacterWithOid]().as[CharacterWithOid].map(c => (c.name, c.age)).collect()
    originalData.map(c => (c.name, c.age)) should contain theSameElementsAs savedData
  }

  it should "be able to set only the data in the Dataset to the collection" in withSparkContext() { sc =>
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("replaceDocument" -> "false"))

    val originalData = characters.map(doc => CharacterWithOid(None, doc.getString("name"),
      if (doc.containsKey("age")) Some(doc.getInteger("age")) else None))

    originalData.toDS().saveToMongoDB()

    sparkSession.read.mongo[CharacterUpperCaseNames]().as[CharacterUpperCaseNames]
      .map(c => CharacterUpperCaseNames(c._id, c.name.toUpperCase())).saveToMongoDB(writeConfig)

    val savedData = sparkSession.read.mongo[CharacterWithOid]().as[CharacterWithOid].map(c => (c.name, c.age)).collect()
    originalData.map(c => (c.name.toUpperCase(), c.age)) should contain theSameElementsAs savedData
  }

  it should "be able to replace data in sharded collections" in withSparkContext() { sc =>
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val shardKey = """{shardKey1: 1, shardKey2: 1}"""
    shardCollection(collectionName, Document.parse(shardKey))

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val readConfig = ReadConfig(sc.getConf).withOptions(Map("partitionerOptions.shardKey" -> shardKey))
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("replaceDocument" -> "true", "shardKey" -> shardKey))

    val originalData = characters.zipWithIndex.map {
      case (doc: Document, i: Int) =>
        ShardedCharacter(i, i, i, doc.getString("name"), if (doc.containsKey("age")) Some(doc.getInteger("age")) else None)
    }
    originalData.toDS().saveToMongoDB()

    sparkSession.read.mongo[ShardedCharacter](readConfig).as[ShardedCharacter].map(c => c.copy(age = c.age.map(_ + 10))).saveToMongoDB(writeConfig)

    val savedData = sparkSession.read.mongo[ShardedCharacter](readConfig).as[ShardedCharacter].collect()
    originalData.map(c => c.copy(age = c.age.map(_ + 10))) should contain theSameElementsAs savedData
  }

  it should "be able to update data in sharded collections" in withSparkContext() { sc =>
    if (!isSharded) cancel("Not a Sharded MongoDB")
    val shardKey = """{shardKey1: 1, shardKey2: 1}"""
    shardCollection(collectionName, Document.parse(shardKey))

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val readConfig = ReadConfig(sc.getConf).withOptions(Map("partitionerOptions.shardKey" -> shardKey))
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("replaceDocument" -> "false", "shardKey" -> shardKey))

    val originalData = characters.zipWithIndex.map {
      case (doc: Document, i: Int) =>
        ShardedCharacter(i, i, i, doc.getString("name"), if (doc.containsKey("age")) Some(doc.getInteger("age")) else None)
    }
    originalData.toDS().saveToMongoDB()

    sparkSession.read.mongo[ShardedCharacter](readConfig).as[ShardedCharacter].map(c => c.copy(age = c.age.map(_ + 10))).saveToMongoDB(writeConfig)

    val savedData = sparkSession.read.mongo[ShardedCharacter](readConfig).as[ShardedCharacter].collect()
    originalData.map(c => c.copy(age = c.age.map(_ + 10))) should contain theSameElementsAs savedData
  }

  private val expectedSchema: StructType = {
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val nameField: StructField = createStructField("name", DataTypes.StringType, true)
    val ageField: StructField = createStructField("age", DataTypes.IntegerType, true)
    createStructType(Array(_idField, ageField, nameField))
  }

  private val multiTypesSchema = StructType(Seq(
    StructField("int32", IntegerType, nullable = false),
    StructField("int64", LongType, nullable = false),
    StructField("boolean", BooleanType, nullable = false),
    StructField("double", DoubleType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructFields.minKey("minKey", nullable = false),
    StructFields.maxKey("maxKey", nullable = false),
    StructFields.objectId("objectId", nullable = false),
    StructFields.regularExpression("regex", nullable = false),
    StructFields.timestamp("timestamp", nullable = false)
  ))

  private val arrayFieldWithNulls: Seq[Document] = Seq("{_id: 1, a: [1,2,3]}", "{_id: 2, a: []}", "{_id: 3, a: null}").map(Document.parse)
  private val documentFieldWithNulls: Seq[Document] = Seq("{_id: 1, a: {a: 1}}", "{_id: 2, a: {}}", "{_id: 3, a: null}").map(Document.parse)
  private val mixedLong: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1), new Document("a", 1L))
  private val mixedDouble: Seq[Document] = Seq(new Document("a", 1), new Document("a", 1L), new Document("a", 1.0))
  private val objectId = new ObjectId("000000000000000000000000")
  private val allBsonTypesDocument: BsonDocument = {
    val document = new BsonDocument()
    document.put("nullValue", new BsonNull())
    document.put("int32", new BsonInt32(42))
    document.put("int64", new BsonInt64(52L))
    document.put("bool", new BsonBoolean(true))
    document.put("date", new BsonDateTime(1463497097))
    document.put("dbl", new BsonDouble(62.0))
    document.put("decimal", new BsonDecimal128(new Decimal128(BigDecimal(72.01).bigDecimal)))
    document.put("string", new BsonString("spark connector"))
    document.put("minKey", new BsonMinKey())
    document.put("maxKey", new BsonMaxKey())
    document.put("objectId", new BsonObjectId(objectId))
    document.put("code", new BsonJavaScript("int i = 0;"))
    document.put("codeWithScope", new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))))
    document.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"))
    document.put("symbol", new BsonSymbol("ruby stuff"))
    document.put("timestamp", new BsonTimestamp(0x12345678, 5))
    document.put("undefined", new BsonUndefined())
    document.put("binary", new BsonBinary(Array[Byte](5, 4, 3, 2, 1)))
    document.put("oldBinary", new BsonBinary(BsonBinarySubType.OLD_BINARY, Array[Byte](1, 1, 1, 1, 1)))
    document.put("arrayInt", new BsonArray(List(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)).asJava))
    document.put("document", new BsonDocument("a", new BsonInt32(1)))
    document.put("dbPointer", new BsonDbPointer("db.coll", objectId))
    document
  }

  private val multiTypesDocs = List(
    "{\"nullValue\" : null, " +
      "\"int32\" : 41, " +
      "\"int64\" : { \"$numberLong\" : \"51\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double\" : 61.0, " +
      "\"string\" : \"spark connector row 1\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419891, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    // this document has a wrong-type field
    "{\"nullValue\" : null, " +
      "\"int32\" : \"42\", " + // wrong type
      "\"int64\" : { \"$numberLong\" : \"52\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double\" : 62.0, " +
      "\"string\" : \"spark connector row 2\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419992, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    // this document has a missing "int32" field
    "{\"nullValue\" : null, " +
      "\"int32\" : 43, " +
      "\"int64\" : \"53\", " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double\" : 63.0, " +
      "\"string\" : \"spark connector row 3\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419993, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    "{\"nullValue\" : null, " +
      "\"int32\" : 44, " +
      "\"int64\" : { \"$numberLong\" : \"54\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double\" : 64.0, " +
      "\"string\" : \"spark connector row 4\", " +
      "\"stringInt\" : \"1532023012\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419894, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    "{\"nullValue\" : null, " +
      "\"int32\" : 45, " +
      "\"int64\" : { \"$numberLong\" : \"55\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double\" : 65.0, " +
      "\"string\" : \"spark connector row 5\", " +
      "\"stringInt\" : \"1532023012\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419895, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}"

  )

  // scalastyle:on magic.number
}

case class SomeData(_id: Int, count: Int)

case class CharacterWithOid(_id: Option[fieldTypes.ObjectId], name: String, age: Option[Int])

case class CharacterUpperCaseNames(_id: Option[fieldTypes.ObjectId], name: String)

case class ShardedCharacter(_id: Int, shardKey1: Int, shardKey2: Int, name: String, age: Option[Int])
