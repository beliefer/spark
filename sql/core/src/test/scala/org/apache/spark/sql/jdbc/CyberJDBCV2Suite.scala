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

package org.apache.spark.sql.jdbc

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class CyberJDBCV2Suite extends JDBCV2Suite {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "org.apache.spark.sql.CyberSQLExtensions")
    .set("spark.sql.catalog.dataPermissionUrl",
      "http://172.18.1.146:30001/api/security/auth/rowcolumn/external/queryAuth")
    .set("spark.sql.catalog.tenantId", "3166")
    .set("spark.sql.catalog.projectId", "0")
    .set("spark.sql.catalog.env", "5")
    .set("spark.sql.catalog.regionCode", "default")
    .set("spark.sql.catalog.debug", "true")

  // Avoid test JDBCV2Suite.
  override def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = ()

  private def testCyberSQL(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*)(testFun)
  }

  testCyberSQL("Data permission is empty") {
    checkAnswer(sql("SELECT * FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
    checkAnswer(sql("SELECT name FROM h2.test.people GROUP BY name"),
      Seq(Row("fred"), Row("mary")))
  }

  testCyberSQL("Data permission for simple scan") {
    checkAnswer(sql("SELECT * FROM h2.test.employee"), Seq(Row(1, "cathy", null, null, false)))
    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE dept > 1"), Seq())
    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE dept > 0"), Seq(Row("cathy")))
    checkAnswer(sql("SELECT name, salary FROM h2.test.employee WHERE dept > 0"),
      Seq(Row("cathy", null)))
    checkAnswer(
      sql("SELECT dept, salary, name, bonus, is_manager FROM h2.test.employee WHERE dept > 0"),
      Seq(Row(1, null, "cathy", null, false)))
    checkAnswer(sql("SELECT name, char_length(name) FROM h2.test.employee WHERE dept > 0"),
      Seq(Row("cathy", 5)))

    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE bonus > 1000"), Seq(Row("cathy")))
    checkAnswer(sql("SELECT name, salary FROM h2.test.employee WHERE bonus > 1000"),
      Seq(Row("cathy", null)))
    checkAnswer(
      sql("SELECT dept, salary, name, bonus, is_manager FROM h2.test.employee WHERE bonus > 1000"),
      Seq(Row(1, null, "cathy", null, false)))
    checkAnswer(sql("SELECT salary, sqrt(salary) FROM h2.test.employee WHERE bonus > 1000"),
      Seq(Row(null, null)))
  }

  testCyberSQL("Data permission for simple scan with limit") {
    checkAnswer(sql("SELECT * FROM h2.test.employee LIMIT 2"),
      Seq(Row(1, "cathy", null, null, false)))
    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE dept > 1 LIMIT 1"), Seq())
    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE dept > 0 LIMIT 1"), Seq(Row("cathy")))
    checkAnswer(sql("SELECT name, salary FROM h2.test.employee WHERE dept > 0 LIMIT 2"),
      Seq(Row("cathy", null)))
    checkAnswer(
      sql("SELECT dept, salary, name, bonus, is_manager FROM h2.test.employee WHERE dept > 0 " +
        "LIMIT 2"),
      Seq(Row(1, null, "cathy", null, false)))
    checkAnswer(sql("SELECT name, char_length(name) FROM h2.test.employee WHERE dept > 0 LIMIT 1"),
      Seq(Row("cathy", 5)))

    checkAnswer(sql("SELECT name FROM h2.test.employee WHERE bonus > 1000 LIMIT 1"),
      Seq(Row("cathy")))
    checkAnswer(sql("SELECT name, salary FROM h2.test.employee WHERE bonus > 1000 LIMIT 1"),
      Seq(Row("cathy", null)))
    checkAnswer(
      sql("SELECT dept, salary, name, bonus, is_manager FROM h2.test.employee " +
        "WHERE bonus > 1000 LIMIT 1"),
      Seq(Row(1, null, "cathy", null, false)))
    checkAnswer(sql("SELECT salary, sqrt(salary) FROM h2.test.employee " +
      "WHERE bonus > 1000 LIMIT 1"),
      Seq(Row(null, null)))
  }


  testCyberSQL("Data permission for aggregate") {
    checkAnswer(sql("SELECT dept FROM h2.test.employee GROUP BY dept"), Seq(Row(1)))
    checkAnswer(sql("SELECT COUNT(name) FROM h2.test.employee"), Seq(Row(1)))
    checkAnswer(sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept"), Seq(Row(1, 1)))
    checkAnswer(sql("SELECT SUM(char_length(name)) FROM h2.test.employee"), Seq(Row(5)))
    checkAnswer(sql("SELECT dept, SUM(char_length(name)) FROM h2.test.employee GROUP BY dept"),
      Seq(Row(1, 5)))

    checkAnswer(sql("SELECT salary FROM h2.test.employee GROUP BY salary"), Seq(Row(null)))
    checkAnswer(sql("SELECT COUNT(salary) FROM h2.test.employee"), Seq(Row(0)))
    checkAnswer(sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary"),
      Seq(Row(null, 0)))
    checkAnswer(sql("SELECT SUM(salary) FROM h2.test.employee"), Seq(Row(null)))
    checkAnswer(sql("SELECT salary, SUM(bonus) FROM h2.test.employee GROUP BY salary"),
      Seq(Row(null, null)))

    checkAnswer(sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary"),
      Seq(Row(1, null)))
    checkAnswer(sql("SELECT COUNT(name), COUNT(salary) FROM h2.test.employee"), Seq(Row(1, 0)))
    checkAnswer(sql("SELECT dept, COUNT(name), COUNT(bonus) FROM h2.test.employee GROUP BY dept"),
      Seq(Row(1, 1, 0)))
  }

  testCyberSQL("Data permission for aggregate with limit") {
    checkAnswer(sql("SELECT dept FROM h2.test.employee GROUP BY dept LIMIT 1"), Seq(Row(1)))
    checkAnswer(sql("SELECT COUNT(name) FROM h2.test.employee LIMIT 2"), Seq(Row(1)))
    checkAnswer(sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept LIMIT 1"),
      Seq(Row(1, 1)))
    checkAnswer(sql("SELECT SUM(char_length(name)) FROM h2.test.employee LIMIT 1"), Seq(Row(5)))
    checkAnswer(sql("SELECT dept, SUM(char_length(name)) FROM h2.test.employee GROUP BY dept " +
      "LIMIT 1"), Seq(Row(1, 5)))

    checkAnswer(sql("SELECT salary FROM h2.test.employee GROUP BY salary LIMIT 1"), Seq(Row(null)))
    checkAnswer(sql("SELECT COUNT(salary) FROM h2.test.employee LIMIT 1"), Seq(Row(0)))
    checkAnswer(sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary LIMIT 1"),
      Seq(Row(null, 0)))
    checkAnswer(sql("SELECT SUM(salary) FROM h2.test.employee LIMIT 1"), Seq(Row(null)))
    checkAnswer(sql("SELECT salary, SUM(bonus) FROM h2.test.employee GROUP BY salary LIMIT 1"),
      Seq(Row(null, null)))

    checkAnswer(sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary LIMIT 1"),
      Seq(Row(1, null)))
    checkAnswer(sql("SELECT COUNT(name), COUNT(salary) FROM h2.test.employee LIMIT 1"),
      Seq(Row(1, 0)))
    checkAnswer(sql("SELECT dept, COUNT(name), COUNT(bonus) FROM h2.test.employee GROUP BY dept " +
      "LIMIT 1"), Seq(Row(1, 1, 0)))
  }

  testCyberSQL("Data permission for aggregate with having") {
    checkAnswer(sql("SELECT dept FROM h2.test.employee GROUP BY dept HAVING dept > 1"), Seq())
    checkAnswer(
      sql("SELECT dept FROM h2.test.employee GROUP BY dept HAVING dept > 0"), Seq(Row(1)))
    checkAnswer(
      sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept HAVING dept > 1"), Seq())
    checkAnswer(
      sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept HAVING dept > 0"),
      Seq(Row(1, 1)))
    checkAnswer(
      sql("SELECT dept, COUNT(name) AS cn FROM h2.test.employee GROUP BY dept HAVING cn > 0"),
      Seq(Row(1, 1)))
    checkAnswer(
      sql("SELECT dept, COUNT(name) AS cn FROM h2.test.employee GROUP BY dept HAVING cn > 1"),
      Seq())

    checkAnswer(
      sql("SELECT salary FROM h2.test.employee GROUP BY salary HAVING salary > 10000"), Seq())
    checkAnswer(
      sql("SELECT salary FROM h2.test.employee GROUP BY salary HAVING salary > 8000"), Seq())
    checkAnswer(
      sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 10000"), Seq())
    checkAnswer(
      sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 8000"), Seq())
    checkAnswer(
      sql("SELECT salary, COUNT(bonus) AS cn FROM h2.test.employee GROUP BY salary " +
        "HAVING cn > 0"), Seq())
    checkAnswer(
      sql("SELECT dept, COUNT(bonus) AS cn FROM h2.test.employee GROUP BY dept " +
        "HAVING cn > 0"), Seq())
    checkAnswer(
      sql("SELECT salary, COUNT(name) AS cn FROM h2.test.employee GROUP BY salary " +
        "HAVING cn > 0"), Seq(Row(null, 1)))

    checkAnswer(
      sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary HAVING dept > 1"),
      Seq())
    checkAnswer(
      sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary HAVING dept > 0"),
      Seq(Row(1, null)))
    checkAnswer(
      sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary HAVING salary > 10000"),
      Seq())
  }

    testCyberSQL("Data permission for aggregate with having and limit") {
      checkAnswer(sql("SELECT dept FROM h2.test.employee GROUP BY dept HAVING dept > 1 LIMIT 1"),
        Seq())
      checkAnswer(sql("SELECT dept FROM h2.test.employee GROUP BY dept HAVING dept > 0 LIMIT 1"),
        Seq(Row(1)))
      checkAnswer(sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept " +
        "HAVING dept > 1 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT dept, COUNT(name) FROM h2.test.employee GROUP BY dept " +
        "HAVING dept > 0 LIMIT 1"), Seq(Row(1, 1)))
      checkAnswer(sql("SELECT dept, COUNT(name) AS cn FROM h2.test.employee GROUP BY dept " +
        "HAVING cn > 0 LIMIT 1"), Seq(Row(1, 1)))
      checkAnswer(sql("SELECT dept, COUNT(name) AS cn FROM h2.test.employee GROUP BY dept " +
        "HAVING cn > 1 LIMIT 1"), Seq())

      checkAnswer(sql("SELECT salary FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 10000 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT salary FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 8000 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 10000 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT salary, COUNT(bonus) FROM h2.test.employee GROUP BY salary " +
        "HAVING salary > 8000 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT salary, COUNT(bonus) AS cn FROM h2.test.employee GROUP BY salary " +
        "HAVING cn > 0 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT dept, COUNT(bonus) AS cn FROM h2.test.employee GROUP BY dept " +
        "HAVING cn > 0 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT salary, COUNT(name) AS cn FROM h2.test.employee GROUP BY salary " +
        "HAVING cn > 0 LIMIT 1"), Seq(Row(null, 1)))

      checkAnswer(sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary " +
        "HAVING dept > 1 LIMIT 1"), Seq())
      checkAnswer(sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary " +
        "HAVING dept > 0 LIMIT 1"), Seq(Row(1, null)))
      checkAnswer(sql("SELECT dept, salary FROM h2.test.employee GROUP BY dept, salary " +
        "HAVING salary > 10000 LIMIT 1"), Seq())
    }
}
