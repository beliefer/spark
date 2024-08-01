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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils
import org.json4s.JsonAST
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{CastSupport, ColumnResolutionHelper}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet, BinaryComparison, Literal, NamedExpression, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Limit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.SparkParserUtils.unescapeSQLString
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Rewrite the logic plan with data permissions.
 */
case class RewriteWithDataPermission(spark: SparkSession) extends Rule[LogicalPlan]
  with SQLConfHelper with CastSupport with ColumnResolutionHelper with PredicateHelper {

  private val processedDataPermissionCheckTag = TreeNodeTag[Unit]("processedDataPermissionCheck")

  // (Catalog Name, Schema Name, Table Name)
  type QueryKey = (String, String, String)

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.getTagValue(processedDataPermissionCheckTag).isDefined) {
      plan
    } else {
      val permissionMap = mutable.Map.empty[QueryKey, JsonAST.JValue]
      val filteredPlan = plan.resolveOperatorsUp {
        case relation: DataSourceV2Relation =>
          val queryKey = findQueryKey(relation)
          val jsonString = retrieveDataPermissions(queryKey._1, queryKey._2, queryKey._3)
          logInfo(s"The json response info is $jsonString")
          val permissionDataOpt = parseDataPermission(jsonString, queryKey)
          permissionDataOpt.map { permissionData =>
            permissionMap(queryKey) = permissionData
            parseFilterPermission(permissionData, relation)
          }.getOrElse(relation)
      }

      val projectedPlan = ignoreDisabledColumns(filteredPlan, permissionMap)
      projectedPlan.setTagValue(processedDataPermissionCheckTag, ())
      projectedPlan
    }
  }

  private def ignoreDisabledColumns(
      plan: LogicalPlan,
      permissionMap: mutable.Map[QueryKey, JsonAST.JValue]): LogicalPlan = plan match {
    case project @ Project(projectList, child) =>
      val referencesMap = findLineageWithReferences(project.references, child)

      var newProject: Project = project

      referencesMap foreach {
        case (key, buffer) if permissionMap.contains(key) =>
          val permissionData = permissionMap(key)
          parseProjectPermission(permissionData, buffer, child).foreach { project =>
            val attrMap = buffer.zip(project.projectList).filterNot {
              case (l, r) => l.semanticEquals(r)
            }.toMap
            val newProjectList = projectList.map { expr =>
              expr transform {
                case attr: Attribute if attrMap.contains(attr) => attrMap(attr)
              }
            }.asInstanceOf[Seq[NamedExpression]]
            newProject = newProject.copy(newProjectList, project)
          }

        case _ =>
      }

      newProject

    case aggregate @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
      val referencesMap = findLineageWithReferences(aggregate.references, child)

      var newAggregate: Aggregate = aggregate

      referencesMap foreach {
        case (key, buffer) if permissionMap.contains(key) =>
          val permissionData = permissionMap(key)
          parseProjectPermission(permissionData, buffer, child).foreach { project =>
            val attrMap = buffer.zip(project.projectList).filterNot {
              case (l, r) => l.semanticEquals(r)
            }.toMap
            val newGroupingExpressions = groupingExpressions.map { expr =>
              expr transform {
                case attr: Attribute if attrMap.contains(attr) => attrMap(attr)
              }
            }
            val newAggregateExpressions = aggregateExpressions.map { expr =>
              expr transform {
                case attr: Attribute if attrMap.contains(attr) => attrMap(attr)
              }
            }.asInstanceOf[Seq[NamedExpression]]
            newAggregate = newAggregate.copy(
              newGroupingExpressions, newAggregateExpressions, project)
          }

        case _ =>
      }

      newAggregate

    case filter @ Filter(condition, child) =>
      val newChild = ignoreDisabledColumns(child, permissionMap)
      val attrMap = child.output.zip(newChild.output).toMap
      val newCondition = condition transform {
        case attr: Attribute if attrMap.contains(attr) => attrMap(attr)
      }
      filter.copy(newCondition, newChild)

    case Limit(l, child) =>
      Limit(l, ignoreDisabledColumns(child, permissionMap))

    case other => other // OK
  }

  private def findQueryKey(relation: DataSourceV2Relation): QueryKey = {
    (relation.catalog, relation.identifier) match {
      case (Some(catalogPlugin), Some(ident)) if ident.namespace().length == 1 =>
        (catalogPlugin.name(), ident.namespace().head, ident.name())
      case _ =>
        throw new AnalysisException(
          s"Unable to retrieve catalog and schema/database from $relation")
    }
  }

  /**
   * Find the lineage with references.
   */
  private def findLineageWithReferences(
      references: AttributeSet,
      plan: LogicalPlan): mutable.Map[QueryKey, mutable.Buffer[Attribute]] = {
    val referencesMap = mutable.Map.empty[QueryKey, mutable.Buffer[Attribute]]
    references.toSeq.foreach {
      case attr: Attribute =>
        findExpressionAndTrackLineageDown(attr, plan).foreach {
          case (a: Attribute, r: DataSourceV2Relation) =>
            val queryKey = findQueryKey(r)
            val buffer = referencesMap.getOrElseUpdate(
              queryKey, mutable.ArrayBuffer.empty[Attribute])
            buffer += a
          case other =>
            throw new AnalysisException(s"Data permission check doesn't support $other")
        }
      case _ =>
    }
    referencesMap
  }

  private val responseExample =
    """
      |{
      |  "code": "200",
      |  "msg": "OK",
      |  "data": [
      |    {
      |      "catalogName": "h2",
      |      "schemaName": "test",
      |      "tableName": "employee",
      |      "disabledColumns": [
      |        "salary", "bonus"
      |      ],
      |      "whereConditions": [
      |        {
      |          "col": "salary",
      |          "operator": "=",
      |          "value": "9000"
      |        },
      |        {
      |          "col": "salary",
      |          "operator": "=",
      |          "value": "10000"
      |        },
      |        {
      |          "col": "is_manager",
      |          "operator": "=",
      |          "value": "false"
      |        },
      |        {
      |          "col": "name",
      |          "operator": "=",
      |          "value": "cathy"
      |        }
      |      ]
      |    },
      |    {
      |      "catalogName": "test_catalog2",
      |      "schemaName": "s2",
      |      "tableName": "tb2",
      |      "disabledColumns": [
      |        "c3","c4"
      |      ],
      |      "whereConditions": [
      |        {
      |          "col": "c3",
      |          "operator": "=",
      |          "value": "1"
      |        },
      |        {
      |          "col": "c4",
      |          "operator": "=",
      |          "value": "x"
      |        }
      |      ]
      |    }
      |  ]
      |}
      |""".stripMargin

  private def retrieveDataPermissions(
      catalogName: String,
      schemaName: String,
      tableName: String): String = {
    if (conf.getConfString("spark.sql.catalog.debug") == "true") {
      return responseExample
    }
    // Get the permissions from service.
    val dataPermissionUrl = conf.getConfString("spark.sql.catalog.dataPermissionUrl")
    val tenantId = conf.getConfString("spark.sql.catalog.tenantId").toInt
    val projectId = conf.getConfString("spark.sql.catalog.projectId").toLong
    val userId = conf.getConfString("spark.sql.catalog.userId").toLong
    val env = conf.getConfString("spark.sql.catalog.env").toInt
    val regionCode = conf.getConfString("spark.sql.catalog.regionCode")
    val httpPost = new HttpPost(dataPermissionUrl)
    httpPost.setHeader("content-type", "application/json")

    val metadata = ("catalogName" -> catalogName) ~
      ("datasourceName" -> "") ~
      ("schemaName" -> schemaName) ~
      ("tableName" -> tableName)
    val jsonArray = JsonAST.JArray(List(metadata))
    val param = ("tenantId" -> tenantId) ~ ("projectId" -> projectId) ~ ("userId" -> userId) ~
      ("env" -> env) ~ ("regionCode" -> regionCode) ~ ("list" -> jsonArray)
    val jsonParam = compact(param)
    logInfo(s"The json request info is $jsonParam")

    val entity = new StringEntity(jsonParam, "utf-8")
    httpPost.setEntity(entity)
    val httpClient = HttpClients.createDefault()
    try {
      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == 200) {
        val entity = response.getEntity()
        val result = EntityUtils.toString(entity)
        result
      } else {
        throw new AnalysisException(s"The status code of response is: $statusCode")
      }
    } catch {
      case ae: AnalysisException => throw ae
      case e: Exception =>
        throw new AnalysisException(s"Get data permission from service failed: $e")
    } finally {
      httpClient.close()
    }
  }

  private def parseDataPermission(jsonString: String, key: QueryKey): Option[JsonAST.JValue] = {
    try {
      val jsonVal: org.json4s.JValue = parse(jsonString)
      jsonVal match {
        case jsonObj: JsonAST.JObject if jsonObj.obj.length == 3 =>
          val code = compact(jsonObj \ "code")
          if (unescapeSQLString(code) == "200") {
            val msg = compact(jsonObj \ "msg")
            logDebug(s"Response code: $code, msg: ${unescapeSQLString(msg)}")
            val data = jsonObj \ "data"
            val jArray = data.children
            val filtered = jArray.filter { ele =>
              val catalogName = unescapeSQLString(compact(ele \ "catalogName"))
              val schemaName = unescapeSQLString(compact(ele \ "schemaName"))
              val tableName = unescapeSQLString(compact(ele \ "tableName"))
              key._1 == catalogName && key._2 == schemaName && key._3 == tableName
            }
            if (filtered.isEmpty) {
              logDebug(s"Can't find the related permission info for $key")
              return None
            }
            if (filtered.length > 1) {
              throw new AnalysisException(s"Multiple related permission infos for $key is illegal")
            }
            Some(filtered.head)
          } else {
            throw new AnalysisException(s"The response code: $code is not excepted")
          }
      }
    } catch {
      case ae: AnalysisException => throw ae
      case e: Exception =>
        throw new AnalysisException(s"Parse the response entity failed: $e")
    }
  }

  private def parseFilterPermission(
      permissionData: JsonAST.JValue, v2Relation: DataSourceV2Relation): LogicalPlan = {
    val whereConditions = permissionData \ "whereConditions"
    if (whereConditions.children.nonEmpty) {
      val functions = mutable.ArrayBuffer.empty[BinaryComparison]
      whereConditions.children.map { condition =>
        val col = unescapeSQLString(compact(condition \ "col"))
        val operator = unescapeSQLString(compact(condition \ "operator"))
        val value = unescapeSQLString(compact(condition \ "value"))
        val colExpr = resolveExpressionByPlanOutput(
          CatalystSqlParser.parseExpression(col), v2Relation)
        val valueExpr = cast(Literal.create(value), colExpr.dataType)
        assert(colExpr.resolved && valueExpr.resolved)
        val funcIdentifier = FunctionIdentifier(operator)
        val resolvedFunc = spark.sessionState.functionRegistry.lookupFunction(
          funcIdentifier, Seq(colExpr, valueExpr))
        assert(resolvedFunc.isInstanceOf[BinaryComparison])
        functions += resolvedFunc.asInstanceOf[BinaryComparison]
      }
      val groupedPredicates = functions.groupBy(_.left)
      val finalPredicates = groupedPredicates.map { case (_, buffer) =>
        buffer.reduceLeft(Or)
      }.reduceLeft(And)
      Filter(finalPredicates, v2Relation)
    } else {
      v2Relation
    }
  }

  private def parseProjectPermission(
      permissionData: JsonAST.JValue,
      buffer: mutable.Buffer[Attribute],
      plan: LogicalPlan): Option[Project] = {
    val disabledColumns = permissionData \ "disabledColumns"
    if (disabledColumns.children.isEmpty) {
      return None
    }
    val disabledAttrNames = disabledColumns.children.map { e =>
      unescapeSQLString(compact(e))
    }
    val newProjectList = buffer.map { attr =>
      if (disabledAttrNames.exists(_.equalsIgnoreCase(attr.name))) {
        Alias(Literal.create(null, attr.dataType), attr.name)()
      } else {
        attr
      }
    }
    Some(Project(newProjectList, plan))
  }
}
