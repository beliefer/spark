/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.sql.DriverManager
import java.util

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.{catalog, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{caseSensitiveResolution, Analyzer, FunctionRegistry, Resolver, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.{ReplaceExpressions, RewriteWithExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connector.catalog.{CatalogManager, Column, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

// scalastyle:off
/**
 * This test uses a corpus of queries ([[proto.Relation]] relations) and transforms each query
 * into its catalyst representation. The resulting catalyst plan is compared with a golden file.
 *
 * The objective of this test is to make sure the JVM client and potentially others produce valid
 * plans, and that these plans are transformed into their expected shape. Additionally this test
 * should capture breaking proto changes to a degree.
 *
 * The corpus of queries is generated by the `PlanGenerationTestSuite` in the connect/client/jvm
 * module.
 *
 * If you need to re-generate the golden files, you need to set the SPARK_GENERATE_GOLDEN_FILES=1
 * environment variable before running this test, e.g.:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "connect/testOnly org.apache.spark.sql.connect.ProtoToParsedPlanTestSuite"
 * }}}
 *
 * If you need to clean the orphaned golden files, you need to set the
 * SPARK_CLEAN_ORPHANED_GOLDEN_FILES=1 environment variable before running this test, e.g.:
 * {{{
 *   SPARK_CLEAN_ORPHANED_GOLDEN_FILES=1 build/sbt "connect/testOnly org.apache.spark.sql.connect.ProtoToParsedPlanTestSuite"
 * }}}
 * Note: not all orphaned golden files should be cleaned, some are reserved for testing backups
 * compatibility.
 */
// scalastyle:on
class ProtoToParsedPlanTestSuite
    extends SparkFunSuite
    with SharedSparkSession
    with ResourceHelper {

  private val cleanOrphanedGoldenFiles: Boolean =
    System.getenv("SPARK_CLEAN_ORPHANED_GOLDEN_FILES") == "1"

  val url = "jdbc:h2:mem:testdb0"
  var conn: java.sql.Connection = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    Utils.classForName("org.h2.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    val properties = new util.Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn
      .prepareStatement(
        "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)")
      .executeUpdate()
    conn
      .prepareStatement("create table test.timetypes (a TIME, b DATE, c TIMESTAMP(7))")
      .executeUpdate()
    conn
      .prepareStatement(
        "create table test.emp(name TEXT(32) NOT NULL," +
          " theid INTEGER, \"Dept\" INTEGER)")
      .executeUpdate()
    conn.commit()
  }

  override def afterAll(): Unit = {
    conn.close()
    if (cleanOrphanedGoldenFiles) {
      cleanOrphanedGoldenFile()
    }
    super.afterAll()
  }

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        Connect.CONNECT_EXTENSIONS_RELATION_CLASSES.key,
        "org.apache.spark.sql.connect.plugin.ExampleRelationPlugin")
      .set(
        Connect.CONNECT_EXTENSIONS_EXPRESSION_CLASSES.key,
        "org.apache.spark.sql.connect.plugin.ExampleExpressionPlugin")
      .set(org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED.key, false.toString)
      .set(org.apache.spark.sql.internal.SQLConf.USE_COMMON_EXPR_ID_FOR_ALIAS.key, false.toString)
  }

  protected val suiteBaseResourcePath = commonResourcePath.resolve("query-tests")
  protected val inputFilePath: Path = suiteBaseResourcePath.resolve("queries")
  protected val goldenFilePath: Path = suiteBaseResourcePath.resolve("explain-results")
  private val emptyProps: util.Map[String, String] = util.Collections.emptyMap()

  private val analyzer = {
    val inMemoryCatalog = new InMemoryCatalog
    inMemoryCatalog.initialize("primary", CaseInsensitiveStringMap.empty())
    inMemoryCatalog.createNamespace(Array("tempdb"), emptyProps)
    inMemoryCatalog.createTable(
      Identifier.of(Array("tempdb"), "myTable"),
      Array(Column.create("id", LongType)),
      Array.empty[Transform],
      emptyProps)
    inMemoryCatalog.createTable(
      Identifier.of(Array("tempdb"), "myStreamingTable"),
      Array(Column.create("id", LongType)),
      Array.empty[Transform],
      emptyProps)

    val catalogManager = new CatalogManager(
      inMemoryCatalog,
      new SessionCatalog(
        new catalog.InMemoryCatalog(),
        FunctionRegistry.builtin,
        TableFunctionRegistry.builtin))
    catalogManager.setCurrentCatalog("primary")
    catalogManager.setCurrentNamespace(Array("tempdb"))

    new Analyzer(catalogManager) {
      override def resolver: Resolver = caseSensitiveResolution
    }
  }

  // Create the tests.
  Files.walkFileTree(
    inputFilePath,
    new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        createTest(file)
        FileVisitResult.CONTINUE
      }
    })

  private def createTest(file: Path): Unit = {
    val relativePath = inputFilePath.relativize(file)
    val fileName = relativePath.getFileName.toString
    if (!fileName.endsWith(".proto.bin")) {
      logError(log"Skipping ${MDC(PATH, fileName)}")
      return
    }
    // TODO: enable below by SPARK-49487
    if (fileName.contains("transpose")) {
      logError(log"Skipping ${MDC(PATH, fileName)} because of SPARK-49487")
      return
    }
    val name = fileName.stripSuffix(".proto.bin")
    test(name) {
      val relation = readRelation(file)
      val planner = new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
      val catalystPlan =
        analyzer.executeAndCheck(planner.transformRelation(relation), new QueryPlanningTracker)
      val finalAnalyzedPlan = {
        object Helper extends RuleExecutor[LogicalPlan] {
          val batches =
            Batch("Finish Analysis", Once, ReplaceExpressions) ::
              Batch("Rewrite With expression", Once, RewriteWithExpression) :: Nil
        }
        Helper.execute(catalystPlan)
      }
      val actual = withSQLConf(SQLConf.MAX_TO_STRING_FIELDS.key -> Int.MaxValue.toString) {
        removeMemoryAddress(normalizeExprIds(finalAnalyzedPlan).treeString)
      }
      val goldenFile = goldenFilePath.resolve(relativePath).getParent.resolve(name + ".explain")
      Try(readGoldenFile(goldenFile)) match {
        case Success(expected) if expected == actual => // Test passes.
        case Success(_) if regenerateGoldenFiles =>
          logInfo("Overwriting golden file.")
          writeGoldenFile(goldenFile, actual)
        case Success(expected) =>
          fail(s"""
               |Expected and actual plans do not match:
               |
               |=== Expected Plan ===
               |$expected
               |
               |=== Actual Plan ===
               |$actual
               |""".stripMargin)
        case Failure(_) if regenerateGoldenFiles =>
          logInfo("Writing golden file.")
          writeGoldenFile(goldenFile, actual)
        case Failure(_) =>
          fail(
            "No golden file found. Please re-run this test with the " +
              "SPARK_GENERATE_GOLDEN_FILES=1 environment variable set")
      }
    }
  }

  private def cleanOrphanedGoldenFile(): Unit = {
    val orphans = Utils
      .recursiveList(goldenFilePath.toFile)
      .filter(g => g.getAbsolutePath.endsWith(".explain"))
      .filter(g => !testNames.contains(g.getName.stripSuffix(".explain")))
    orphans.foreach(Utils.deleteRecursively)
  }

  private def removeMemoryAddress(expr: String): String = {
    expr
      .replaceAll("@[0-9a-f]+,", ",")
      .replaceAll("@[0-9a-f]+\\)", ")")
  }

  private def readRelation(path: Path): proto.Relation = {
    val input = Files.newInputStream(path)
    try proto.Relation.parseFrom(input)
    finally {
      input.close()
    }
  }

  private def readGoldenFile(path: Path): String = {
    removeMemoryAddress(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
  }

  private def writeGoldenFile(path: Path, value: String): Unit = {
    val writer = Files.newBufferedWriter(path)
    try writer.write(value)
    finally {
      writer.close()
    }
  }
}
