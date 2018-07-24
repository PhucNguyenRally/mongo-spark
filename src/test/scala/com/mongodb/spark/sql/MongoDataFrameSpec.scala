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
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import com.mongodb.ConnectionString
import com.mongodb.async.SingleResultCallback
import com.mongodb.MongoClient
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.exceptions.MongoTypeConversionException
import org.apache.spark.SparkException
import org.bson._
import org.bson.types.ObjectId
import com.mongodb.spark.{MongoSpark, _}
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql.types.BsonCompatibility
import org.apache.spark.sql.types.DataTypes
import com.mongodb.spark.sql.helpers.StructFields

class MongoDataFrameSpec extends RequiresMongoDB {
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

  def invalidRowHandler(invalidRows: Seq[Row]): Unit = {
    invalidRows.foreach(println)
  }

  "DataFrameReader" should "should be easily created from the SQLContext and load from Mongo" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val df = SQLContext.getOrCreate(sc).read.mongo()

    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "handle selecting out of order columns" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.mongo()

    df.select("name", "age").orderBy("age").rdd.map(r => (r.get(0), r.get(1))).collect() should
      equal(characters.sortBy(_.getInteger("age", 0)).map(doc => (doc.getString("name"), doc.getInteger("age"))))
  }

  it should "handle mixed numerics with long precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedLong).saveToMongoDB()
    val expectedData = List(1L, 1L, 1L)
    val df = SQLContext.getOrCreate(sc).read.mongo().select("a")

    df.count() should equal(3)
    df.collect().map(r => r.get(0)) should equal(expectedData)

    val cached = df.cache()
    cached.count() should equal(3)
    cached.collect().map(r => r.get(0)) should equal(expectedData)
  }

  it should "handle mixed numerics with double precedence" in withSparkContext() { sc =>
    sc.parallelize(mixedDouble).saveToMongoDB()
    val expectedData = List(1.0, 1.0, 1.0)
    val df = SQLContext.getOrCreate(sc).read.mongo().select("a")

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

    SQLContext.getOrCreate(sc).read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD().collect().toList should equal(arrayFieldWithNulls)
  }

  it should "handle document fields with null values" in withSparkContext() { sc =>
    sc.parallelize(documentFieldWithNulls).saveToMongoDB()

    val saveToCollectionName = s"${collectionName}_new"
    val readConf = readConfig.withOptions(Map("collection" -> saveToCollectionName))

    SQLContext.getOrCreate(sc).read.mongo().write.mode(SaveMode.Overwrite).option("collection", saveToCollectionName).mongo()
    MongoSpark.builder().sparkContext(sc).readConfig(readConfig).build().toRDD().collect().toList should equal(documentFieldWithNulls)
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()

    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.mongo[Character]()
    val reflectedSchema: StructType = ScalaReflection.schemaFor[Character].dataType.asInstanceOf[StructType]

    df.schema should equal(reflectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)
  }

  it should "include any pipelines when inferring the schema" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()
    val sqlContext = SQLContext.getOrCreate(sc)

    var df = sqlContext.read.option("pipeline", "[{ $match: { name: { $exists: true } } }]").mongo()
    df.schema should equal(expectedSchema)
    df.count() should equal(10)
    df.filter("age > 100").count() should equal(6)

    df = sqlContext.read.option("pipeline", "[{ $project: { _id: 1, age: 1 } }]").mongo()
    df.schema should equal(createStructType(expectedSchema.fields.filter(p => p.name != "name")))
  }

  it should "throw an exception if pipeline is invalid" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    sc.parallelize(List("{counter: 1}", "{counter: 2}", "{counter: 3}").map(Document.parse)).saveToMongoDB()

    an[IllegalArgumentException] should be thrownBy SQLContext.getOrCreate(sc).read.option("pipeline", "[1, 2, 3]").mongo()
  }

  "DataFrameWriter" should "be easily created from a DataFrame and save to Mongo" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo()

    sqlContext.read.mongo[Character]().count() should equal(9)
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val saveToCollectionName = s"${collectionName}_new"
    val writeConfig = WriteConfig(sc.getConf).withOptions(Map("collection" -> saveToCollectionName))

    sc.parallelize(characters)
      .filter(_.containsKey("age")).map(doc => Character(doc.getString("name"), doc.getInteger("age")))
      .toDF().write.mongo(writeConfig)

    sqlContext.read.option("collection", saveToCollectionName).mongo[Character]().count() should equal(9)
  }

  it should "support INSERT INTO SELECT statements" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.mongo[Character]()

    df.registerTempTable("people")
    sqlContext.sql("INSERT INTO table people SELECT 'Mort', 1000")

    sqlContext.read.mongo().count() should equal(1)
  }

  it should "support INSERT OVERWRITE SELECT statements" in withSparkContext() { sc =>
    sc.parallelize(characters).saveToMongoDB()
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.mongo[Character]()

    df.registerTempTable("people")
    sqlContext.sql("INSERT OVERWRITE table people SELECT 'Mort', 1000")

    sqlContext.read.mongo().count() should equal(1)
  }

  "DataFrames" should "round trip all bson types" in withSparkContext() { sc =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val newCollectionName = s"${collectionName}_new"
    SQLContext.getOrCreate(sc).read.mongo().write.option("collection", newCollectionName).mongo()

    val original = database.getCollection(collectionName).find().iterator().asScala.toList
    val copied = database.getCollection(newCollectionName).find().iterator().asScala.toList
    copied should equal(original)
  }

  it should "be able to cast all types to a string value" in withSparkContext() { sc =>
    database.getCollection(collectionName, classOf[BsonDocument]).insertOne(allBsonTypesDocument)
    val bsonValuesAsStrings = sc.loadFromMongoDB().toDS[BsonValuesAsStringClass]().first()

    val expected = BsonValuesAsStringClass(
      nullValue = "null",
      int32 = "42",
      int64 = "52",
      boolean = "true",
      date = """{ "$date" : 1463497097 }""",
      double = "62.0",
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
      binary = """{ "$binary" : "BQQDAgE=", "$type" : "0" }""",
      oldBinary = """{ "$binary" : "AQEBAQE=", "$type" : "2" }""",
      arrayInt = "[1, 2, 3]",
      document = """{ "a" : 1 }""",
      dbPointer = """{ "$ref" : "db.coll", "$id" : { "$oid" : "000000000000000000000000" } }"""
    )
    bsonValuesAsStrings should equal(expected)
  }

  it should "be able to cast all types to a string value with local mongodb" in withSparkContext() { sc =>
    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("testTypes").drop()
    myDb.getCollection("testTypes", classOf[BsonDocument]).insertOne(allBsonTypesDocument)

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "testTypes"), Some(ReadConfig(sc)))

    val bsonValuesAsStrings = sc.loadFromMongoDB(readConfigMap).toDS[BsonValuesAsStringClass]().first()

    val expected = BsonValuesAsStringClass(
      nullValue = "null",
      int32 = "42",
      int64 = "52",
      boolean = "true",
      date = """{ "$date" : 1463497097 }""",
      double = "62.0",
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
      binary = """{ "$binary" : "BQQDAgE=", "$type" : "0" }""",
      oldBinary = """{ "$binary" : "AQEBAQE=", "$type" : "2" }""",
      arrayInt = "[1, 2, 3]",
      document = """{ "a" : 1 }""",
      dbPointer = """{ "$ref" : "db.coll", "$id" : { "$oid" : "000000000000000000000000" } }"""
    )
    bsonValuesAsStrings should equal(expected)
  }

  it should "be able to retrieve multi-types rec with a specified schema from local mongodb" in withSparkContext() { sc =>
    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test2").insertOne(Document.parse(typesDoc))

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2"), Some(ReadConfig(sc)))

    val multiTypesRow = sc.loadFromMongoDB(readConfigMap).toDS[MultiTypesSchema]().first()

    multiTypesRow.nullValue should equal(None)
    multiTypesRow.nothing should equal(None)
    multiTypesRow.int32 should equal(42)
    multiTypesRow.int64 should equal(52)
    multiTypesRow.boolean should equal(true)
    multiTypesRow.double1 should equal(62.0)
    multiTypesRow.objectId.oid should equal("000000000000000000000000")
    multiTypesRow.regex.regex should equal("^test.*regex.*xyz$")
    multiTypesRow.timestamp.time should equal(305419896)
    multiTypesRow.timestamp.inc should equal(5)
  }

  it should "be able to silently do conversions between Long, Int, and Double types from local mongodb" in withSparkContext() { sc =>
    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2"), Some(ReadConfig(sc)))

    val multiTypesRow = sc.loadFromMongoDB(readConfigMap).toDS[SwapLongIntSchema]().first()

    multiTypesRow.nullValue should equal(None)
    multiTypesRow.nothing should equal(None)
    multiTypesRow.int32 should equal(42)
    multiTypesRow.int64 should equal(52)
    multiTypesRow.boolean should equal(true)
    multiTypesRow.double1 should equal(62)
    multiTypesRow.double2 should equal(65)
    multiTypesRow.objectId.oid should equal("000000000000000000000000")
    multiTypesRow.regex.regex should equal("^test.*regex.*xyz$")
    multiTypesRow.timestamp.time should equal(305419896)
    multiTypesRow.timestamp.inc should equal(5)
  }

  it should "throws an exception if using schema with wrong conversion from String to Int" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructField("stringInt", IntegerType, nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from String to ObjectId" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.objectId("stringInt", nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from String to timestamp" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.timestamp("stringInt", nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from int to timestamp" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.timestamp("int64", nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from timestamp to int" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructField("timestamp", LongType, nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from String to Date" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructField("stringDate", DateType, nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from Timestamp to Date" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructField("timestamp", DateType, nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from Date to timestamp" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.timestamp("date", nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "throws an exception if using schema with wrong conversion from String to Regex" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.regularExpression("string", nullable = false)))

    an[SparkException] should be thrownBy MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()
  }

  it should "be able to handle a missing field" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructFields.timestamp("dummy", nullable = false)))

    val dummy = MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()

    dummy.size should equal(1)
    dummy.anyNull should equal(true)

  }

  it should "be handle null integer value using a schema" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val readConfigMap = Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test2")

    val schema = StructType(Seq(StructField("nullValue", IntegerType, nullable = false)))

    val dummy = MongoSpark.read(sqlContext).schema(schema).options(readConfigMap).load().toDF().first()

    dummy.size should equal(1)
    dummy.anyNull should equal(true)

    dummy.getAs[Int](0) should not equal (0)
  }

  it should "be able to retrieve multi-types recs with a specified schema from local mongodb" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test3").drop()

    myDb.getCollection("test3").insertMany(multiTypesDocs.map(doc => Document.parse(doc)).toList.asJava)

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test3"), Some(ReadConfig(sc)))

    val df = MongoSpark.builder().sqlContext(sqlContext).readConfig(readConfigMap).build().toDF(multiTypesSchema)

    df.registerTempTable("test3")

    val dfQuery = sqlContext.sql("select * from test3")
    val testDir = "/tmp/test3"

    import java.io.File
    import scala.reflect.io.Directory

    val directory = new Directory(new File(testDir))
    directory.deleteRecursively()

    an[SparkException] should be thrownBy dfQuery.write.json(testDir)
  }

  it should "be able to retrieve multi-types recs with a specified schema from local mongodb with a callback" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val myClient = new MongoClient()
    val myDb = myClient.getDatabase("test")

    myDb.getCollection("test3").drop()

    myDb.getCollection("test3").insertMany(multiTypesDocs.map(doc => Document.parse(doc)).toList.asJava)
    //myDb.getCollection("test3").insertOne(Document.parse(typesDoc))

    val readConfigMap = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "test3"), Some(ReadConfig(sc)))

    val df = MongoSpark.builder().sqlContext(sqlContext).readConfig(readConfigMap).build().toDF(multiTypesSchema, invalidRowHandler)

    df.registerTempTable("test3")

    val dfQuery = sqlContext.sql("select * from test3")
    val testDir = "/tmp/test3"

    import java.io.File
    import scala.reflect.io.Directory

    val directory = new Directory(new File(testDir))
    directory.deleteRecursively()

    dfQuery.show()

    dfQuery.write.json(testDir)
    // should not have an exception here
  }

  it should "be able to round trip schemas containing MapTypes" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    val characterMap = characters.map(doc => Row(doc.getString("name"), Map("book" -> "The Hobbit", "author" -> "J. R. R. Tolkien")))
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true)
    ))
    val df = sqlContext.createDataFrame(sc.parallelize(characterMap), schema)
    df.write.mongo()

    val savedDF = sqlContext.read.schema(schema).mongo()
    savedDF.collectAsList() should equal(df.collectAsList())
  }

  it should "be able to upsert and replace data in an existing collection" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val originalData = Seq(SomeData(1, 100), SomeData(2, 200), SomeData(3, 300)).toDS()

    originalData.saveToMongoDB()
    MongoSpark.load(sc).toDS[SomeData]().collect() should contain theSameElementsAs originalData.collect()

    val replacementData = Seq(SomeData(1, 1000), SomeData(2, 2000), SomeData(3, 3000)).toDS()

    replacementData.toDF().write.mode("append").mongo()
    MongoSpark.load(sc).toDS[SomeData]().collect() should contain theSameElementsAs replacementData.collect()
  }

  it should "be able to handle optional _id fields when upserting / replacing data in a collection" in withSparkContext() { sc =>
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val originalData = characters.map(doc => CharacterWithOid(None, doc.getString("name"),
      if (doc.containsKey("age")) Some(doc.getInteger("age")) else None))

    originalData.toDF().saveToMongoDB()
    val savedData = sqlContext.read.mongo[CharacterWithOid]().as[CharacterWithOid].map(c => (c.name, c.age)).collect()
    originalData.map(c => (c.name, c.age)) should contain theSameElementsAs savedData
  }

  private val expectedSchema: StructType = {
    val _idField: StructField = createStructField("_id", BsonCompatibility.ObjectId.structType, true)
    val nameField: StructField = createStructField("name", DataTypes.StringType, true)
    val ageField: StructField = createStructField("age", DataTypes.IntegerType, true)
    createStructType(Array(_idField, ageField, nameField))
  }

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
    document.put("boolean", new BsonBoolean(true))
    document.put("date", new BsonDateTime(1463497097))
    document.put("double", new BsonDouble(62.0))
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

  private val multiTypesSimpleSchema = StructType(Seq(
    StructField("int32", IntegerType, nullable = false),
    StructField("boolean", BooleanType, nullable = false),
    StructField("double1", DoubleType, nullable = false),
    StructField("string", StringType, nullable = false)
  ))

  private val multiTypesSchema = StructType(Seq(
    StructField("int32", IntegerType, nullable = false),
    StructField("int64", LongType, nullable = false),
    StructField("boolean", BooleanType, nullable = false),
    StructField("double1", DoubleType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructFields.minKey("minKey", nullable = false),
    StructFields.maxKey("maxKey", nullable = false),
    StructFields.objectId("objectId", nullable = false),
    StructFields.regularExpression("regex", nullable = false),
    StructFields.timestamp("timestamp", nullable = false)
  ))

  private val typesDoc = "{\"nullValue\" : null, " +
    "\"int32\" : 42, " +
    "\"int64\" : { \"$numberLong\" : \"52\" }, " +
    "\"boolean\" : true, " +
    "\"date\" : { \"$date\" : 1463497097 }, " +
    "\"double1\" : 61.0, " +
    "\"double2\" : 65.2, " +
    "\"string\" : \"spark connector\", " +
    "\"stringInt\" : \"1532023007\", " +
    "\"stringDate\" : \"2018-07-15\", " +
    "\"minKey\" : {\"$minKey\" : 1 }, " +
    "\"maxKey\" : {\"$maxKey\" : 1 }, " +
    "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
    "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
    "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
    "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419896, \"i\" : 5 }}, " +
    "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}"

  private val multiTypesDocs = List(
    "{\"nullValue\" : null, " +
      "\"int32\" : 41, " +
      "\"int64\" : { \"$numberLong\" : \"52\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double1\" : 61.0, " +
      "\"double2\" : 65.2, " +
      "\"string\" : \"spark connector row 1\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419896, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    // this document has a wrong-type field
    "{\"nullValue\" : null, " +
      "\"int32\" : \"42\", " + // wrong type
      "\"int64\" : { \"$numberLong\" : \"52\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double1\" : 62.0, " +
      "\"double2\" : 65.2, " +
      "\"string\" : \"spark connector row 2\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419996, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    // this document has a missing "int32" field
    "{\"nullValue\" : null, " +
      "\"int64\" : { \"$numberLong\" : \"52\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double1\" : 63.0, " +
      "\"double2\" : 65.2, " +
      "\"string\" : \"spark connector row 3\", " +
      "\"stringInt\" : \"1532023007\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419996, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}",

    "{\"nullValue\" : null, " +
      "\"int32\" : 44, " +
      "\"int64\" : { \"$numberLong\" : \"52\" }, " +
      "\"boolean\" : true, " +
      "\"date\" : { \"$date\" : 1463497097 }, " +
      "\"double1\" : 64.0, " +
      "\"double2\" : 69.2, " +
      "\"string\" : \"spark connector row 4\", " +
      "\"stringInt\" : \"1532023012\", " +
      "\"stringDate\" : \"2018-07-15\", " +
      "\"minKey\" : {\"$minKey\" : 1 }, " +
      "\"maxKey\" : {\"$maxKey\" : 1 }, " +
      "\"objectId\" : { \"$oid\" : \"000000000000000000000000\" }, " +
      "\"code\" : { \"$code\" : \"int i = 0;\" }, " +
      "\"regex\" : { \"$regex\" : \"^test.*regex.*xyz$\", \"$options\" : \"I\" }, " +
      "\"timestamp\" : { \"$timestamp\" : { \"t\" : 305419890, \"i\" : 5 }}, " +
      "\"oldBinary\" : { \"$binary\" : \"AQEBAQE=\", \"$type\" : \"02\" }}"
  )

  // scalastyle:on magic.number
}

case class SomeData(_id: Int, count: Int)

case class CharacterWithOid(_id: Option[fieldTypes.ObjectId], name: String, age: Option[Int])

case class MultiTypes(int32: Option[Int], int64: Option[Long], boolean: Option[Boolean])

case class MultiTypesSchema(nullValue: Option[Int], nothing: Option[String], int32: Int, int64: Long, boolean: Boolean, double1: Double, double2: Double, string: String, minKey: fieldTypes.MinKey, maxKey: fieldTypes.MaxKey, objectId: fieldTypes.ObjectId, code: fieldTypes.JavaScript, regex: fieldTypes.RegularExpression, timestamp: fieldTypes.Timestamp)

case class SwapLongIntSchema(nullValue: Option[Int], nothing: Option[String], int32: Long, int64: Int, boolean: Boolean, double1: Int, double2: Long, string: String, minKey: fieldTypes.MinKey, maxKey: fieldTypes.MaxKey, objectId: fieldTypes.ObjectId, code: fieldTypes.JavaScript, regex: fieldTypes.RegularExpression, timestamp: fieldTypes.Timestamp)
