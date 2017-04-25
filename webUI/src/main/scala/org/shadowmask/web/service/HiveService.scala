/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.shadowmask.web.service

import java.sql.{Connection, ResultSet}

import com.google.gson.{Gson, JsonObject}
import org.shadowmask.core
import org.shadowmask.core.discovery.DataTypeDiscovery
import org.shadowmask.framework.datacenter.hive._
import org.shadowmask.framework.task._
import org.shadowmask.framework.task.hive.{HiveBatchedTask, HiveExecutionTask, HiveQueryTask}
import org.shadowmask.framework.task.mask.MaskTask
import org.shadowmask.jdbc.connection.ConnectionProvider
import org.shadowmask.jdbc.connection.description.{JDBCConnectionDesc, KerberizedHive2JdbcConnDesc, SimpleHive2JdbcConnDesc}
import org.shadowmask.model.data.TitleType
import org.shadowmask.web.api.MaskRules
import org.shadowmask.web.api.MaskRules._
import org.shadowmask.web.model._

import scala.collection.JavaConverters._

class HiveService {

  implicit def t2Some[T](t: T) = Some[T](t)


  /**
    * get all schema View objects .
    *
    * @return
    */
  def getSchemaViewObject(): SchemaResult = {

    try {
      val dcs = HiveDcs.dcCotainer
      SchemaResult(0, "ok", {
        for (dcName <- dcs.getAllDcNames.asScala.toList) yield {
          SchemaObjectParent(dcName, {
            for (schema <- getAllSchemasByName(dcName)) yield {
              SchemaObject(schema, schema, {
                for (table <- getAllTables(dcName, schema).get._2) yield {
                  TableProp(table, table)
                }
              })
            }
          })
        }
      })
    } catch {
      case e: Exception => SchemaResult(1, s"server internal error: ${e.getMessage}", Nil)
    }

  }


  def getTableViewObject(dcName: String, schemaName: String, tableName: String, limit: Int = 10): TableResult = {
    val data = getTableContents(dcName, schemaName, tableName, limit)
    TableResult(0, "ok", TableContent({
      var i = 0;
      val titleAndValue =
        for ((name, cType) <- getTableTile(dcName, schemaName, tableName).get) yield {
          new javafx.util.Pair[String, String](name, if (data.get.size > 0) {
            i += 1;
            data.get(0)(i - 1)
          } else "")
        }
      val types = DataTypeDiscovery.inspectTypes(titleAndValue.asJava).asScala.toList

      (for (i <- 0 until types.size) yield {
        val t = types(i).name() match {
          case "IDENTIFIER" => TitleType.ID
          case "QUSI_IDENTIFIER" => TitleType.HALF_ID
          case "SENSITIVE" => TitleType.SENSITIVE
          case "NON_SENSITIVE" => TitleType.NONE_SENSITIVE
        }
        TableTitle(titleAndValue(i).getKey, titleAndValue(i).getKey, t.name, t.color)
      }).toList
    }, {
      data
    }))
  }


  /**
    * submit a task async .
    *
    * @param request
    */
  def submitMaskTask(request: MaskRequest): String = {
    val maskSql = getMaskSql(request)
    val dcs = HiveDcs.dcCotainer
    val dc = dcs.getDc(request.dsSource.get)
    val hiveTask = dc match {
      case dc: SimpleHiveDc => new HiveExecutionTask[SimpleHive2JdbcConnDesc] {
        override def sql(): String = maskSql

        override def connectionDesc(): SimpleHive2JdbcConnDesc = conSimpleDc2Desc(dc, "default")
      }
      case dc: KerberizedHiveDc => new HiveExecutionTask[KerberizedHive2JdbcConnDesc] {
        override def sql(): String = maskSql

        override def connectionDesc(): KerberizedHive2JdbcConnDesc = conKrbDc2Desc(dc, "default")
      }
    }
    hiveTask.registerWatcher(new SimpleRollbackWatcher() {
      override def onConnection(connection: Connection): Unit = {
        // todo make this configurable .
        connection.prepareStatement("add jar hdfs:///tmp/udf/shadowmask-core-0.1-SNAPSHOT.jar").execute();
        connection.prepareStatement("add jar hdfs:///tmp/udf/hive-engine-0.1-SNAPSHOT.jar").execute();
        connection.prepareStatement("set hive.execution.engine=spark").execute();
        for ((k, (func, clazz, _)) <- MaskRules.commonFuncMap) {
          val sql = s"CREATE TEMPORARY FUNCTION $func AS '$clazz'"
          connection.prepareStatement(sql).execute();
          println(s"$sql;")
        }
        connection.commit()
      }
    })
    val task = new MaskTask(hiveTask)
    task.setTaskName(request.taskName.get)
    HiveMaskTaskContainer().submitTask(task)
    maskSql
  }

  /**
    * convert a mask request to a sql .
    *
    * @param request
    * @return
    */
  def getMaskSql(request: MaskRequest): String = {
    val ruleByColumn: Map[
      String // column name
      , (
      String // type id 1,2 etc
        , String // rule id Email ,Ip etc .
        , Map[
        String // param name
        , String // param value
        ]
      )
      ] =
      (for (col <- request.rules.get) yield (
        col.colName.get ->(col.rule.get.maskTypeID.get, col.rule.get.maskRuleID.get
          , (for (param <- col.rule.get.maskParams.get) yield (param.paramName.get -> param.paramValue.get)).toMap
          )
        )).toMap

    val columns = getTableTile(request.dsSource.get, request.dsSchema.get, request.dsTable.get) match {
      case None => List()
      case Some(list: List[(String, String)]) => for ((name, _) <- list) yield name
    }
    s"""| CREATE ${
      request.distType.get.toUpperCase() match {
        case "VIEW" => "VIEW"
        case "TABLE" => "TABLE"
        case _ => "table"
      }
    } ${request.distSchema.get}.${request.distName.get} AS SELECT  ${
      columns.map(c => {
        ruleByColumn.get(c) match {
          case None => c
          case Some((_, maskType, paramMap: Map[String, String])) => buildFunction(maskType) match {
            case None => c
            case Some(func) => s"${func.toSql(c, paramMap)} AS $c"
          }
        }
      }).mkString(",")
    } FROM ${request.dsSchema.get}.${request.dsTable.get}""".stripMargin
  }


  /**
    * get task by page
    *
    * @param typpe
    * @param pageNum
    * @param pageSize
    * @return
    */
  def getTaskListByPage(typpe: Int, pageNum: Int, pageSize: Int): TaskResult = {
    TaskResult(0, "ok", {
      val Some((result: List[MaskTask], totalSize)) = HiveMaskTaskContainer().getTaskByPage(typpe, pageNum, pageSize)
      TaskResult_data(totalSize,
        result.map(t => {
          TaskViewObject(t.getTaskName, t.getSubmitTime.toString, t.getFinishTime.toString, t.getExceptedTime.toString)
        }).toList
      )
    })
  }

  /**
    * get all task .
    *
    * @param typpe
    * @return
    */
  def getAllTask(typpe: Int): TaskResult = {
    TaskResult(0, "ok", {
      val Some((result: List[MaskTask], totalSize)) = HiveMaskTaskContainer().getAllTask(typpe)
      TaskResult_data(totalSize,
        result.map(t => {
          TaskViewObject(t.getTaskName, t.getSubmitTime.toString, t.getFinishTime.toString, t.getExceptedTime.toString)
        }).toList
      )
    })
  }


  /**
    * drop a table (view) according its name .
    *
    * @param dcName
    * @param schemaName
    * @param tableName
    */
  def dropTableOrView(dcName: String, schemaName: String, tableName: String): Unit = {
    val dcs = HiveDcs.dcCotainer
    val dc = dcs.getDc(dcName);

    def innerProcess[D <: JDBCConnectionDesc](connection: Connection, parentTask: HiveBatchedTask[D]): Unit = {

      val queryTask = new HiveQueryTask[String, JDBCConnectionDesc] {
        override def collector(): JdbcResultCollector[String] = new JdbcResultCollector[String] {
          override def collect(resultSet: ResultSet): String = {
            if (!resultSet.getString(1).toLowerCase().trim.startsWith("create view")) "table" else "view"
          }
        }

        override def sql(): String = s"show create table $tableName"

        override def connectionDesc(): JDBCConnectionDesc = null
      }
      //must explicit
      parentTask.useConnection(queryTask, connection);
      Executor().executeTaskSync(queryTask)
      val tableType = queryTask.queryResults().get(0);
      val dTask = new HiveExecutionTask[JDBCConnectionDesc] {
        override def sql(): String = s"drop $tableType $tableName"

        override def connectionDesc(): JDBCConnectionDesc = null
      }
      //must explicit
      parentTask.useConnection(dTask, connection)
      Executor().executeTaskSync(dTask)

    }

    val dropTask = dc match {
      case dc: SimpleHiveDc =>
        val desc = conSimpleDc2Desc(dc, schemaName)
        new HiveBatchedTask[SimpleHive2JdbcConnDesc] {
          override def process(connection: Connection): Unit = innerProcess(connection, this)

          override def connectionDesc(): SimpleHive2JdbcConnDesc = desc
        }
      case dc: KerberizedHiveDc =>
        val desc = conKrbDc2Desc(dc, schemaName)
        new HiveBatchedTask[KerberizedHive2JdbcConnDesc] {

          override def process(connection: Connection): Unit = innerProcess(connection, this)

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = desc
        }
    }
    Executor().executeTaskSync(dropTask)

  }


  def getRiskViewObject(dcName: String, schemaName: String, name: String, columns: Array[String]): Option[List[RiskItems]] = {
    caculateRisk(dcName, schemaName, name, columns).flatMap(s => {
      val json = new Gson().fromJson(s._2, classOf[JsonObject])

      val iterator = json.entrySet().iterator()
      var res = List[RiskItems]()
      while (iterator.hasNext) {
        val n = iterator.next()
        res = RiskItems("0",s._1+"_"+n.getKey,n.getValue.toString)::res
      }
      res
    })
  }


  /**
    * caculate privacy disclosure rask .
    *
    * @param dcName
    * @param schemaName
    * @param name
    * @param columns
    */
  def caculateRisk(dcName: String, schemaName: String, name: String, columns: Array[String]): List[(String, String)] = {
    val dcs = HiveDcs.dcCotainer
    val dc = dcs.getDc(dcName)

    var result = List[(String, String)]()
    def innerProcess[D <: JDBCConnectionDesc](connection: Connection, parentTask: HiveBatchedTask[D], d: D): Unit = {
      class PTask(funcName: String) extends HiveQueryTask[String, D] {

        this.withConnectionProvider(new ConnectionProvider[D] {
          override def get(desc: D): Connection = connection

          override def release(connection: Connection): Unit = {}
        })

        override def collector(): JdbcResultCollector[String] = new JdbcResultCollector[String] {
          override def collect(resultSet: ResultSet): String = resultSet.getString(1)
        }

        override def sql(): String = s"SELECT $funcName(${columns.mkString(",")}) FROM $name"

        override def connectionDesc(): D = d
      }
      for ((name, (func, _)) <- MaskRules.evaluateFunc) {
        val task = new PTask(func)
        Executor().executeTaskSync(task)
        result = (name, task.queryResults().get(0)) :: result
      }

    }

    val task = dc match {
      case dc: SimpleHiveDc =>
        val desc = conSimpleDc2Desc(dc, schemaName)
        new HiveBatchedTask[SimpleHive2JdbcConnDesc] {
          override def process(connection: Connection): Unit = innerProcess(connection, this, null)

          override def connectionDesc(): SimpleHive2JdbcConnDesc = desc
        }
      case dc: KerberizedHiveDc =>
        val desc = conKrbDc2Desc(dc, schemaName)
        new HiveBatchedTask[KerberizedHive2JdbcConnDesc] {
          override def process(connection: Connection): Unit = innerProcess(connection, this, null)

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = desc
        }
    }

    task.registerWatcher(new SimpleWatcher {

      //prepare udfs .
      override def onConnection(connection: Connection): Unit = {
        connection.prepareStatement("add jar hdfs:///tmp/udf/shadowmask-core-0.1-SNAPSHOT.jar").execute()
        connection.prepareStatement("add jar hdfs:///tmp/udf/hive-engine-0.1-SNAPSHOT.jar").execute()
        connection.prepareStatement("set hive.execution.engine=spark").execute();
        for ((k, (func, clazz)) <- MaskRules.evaluateFunc) {
          val sql = s"CREATE TEMPORARY FUNCTION $func AS '$clazz'"
          connection.prepareStatement(sql).execute();
          println(s"$sql;")
        }
        connection.commit()
      }
    })
    Executor().executeTaskSync(task)
    result
  }


  def getAllSchemasByName(dcName: String): List[String] = {
    val dcs = HiveDcs.dcCotainer
    getAllSchemas(dcs.getDc(dcName))
  }

  /**
    * get all schema of a data center .
    *
    * @param dc
    * @return
    */
  def getAllSchemas(dc: HiveDc): List[String] = {
    val tableNameCollector = new JdbcResultCollector[String] {
      override def collect(resultSet: ResultSet): String = resultSet.getString(1)
    }
    val querySql = "show databases"
    val task = dc match {
      case dc: KerberizedHiveDc =>
        new HiveQueryTask[String, KerberizedHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector;

          override def sql(): String = querySql

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = getKerberizedDesc(dc)
        }
      case dc: SimpleHiveDc =>
        new HiveQueryTask[String, SimpleHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = querySql

          override def connectionDesc(): SimpleHive2JdbcConnDesc = getSimpleDesc(dc)
        }
    }
    Executor().executeTaskSync(task)
    task.queryResults().asScala.toList
  }


  /**
    * get all datacenter schemas .
    *
    * @return
    */
  def getAllDcSchemas(): Option[List[(String, List[String])]] = {
    val dcNames = HiveDcs.dcCotainer.getAllDcNames.asScala.toList
    dcNames match {
      case lst: List[String] =>
        Some(for (dcName <- lst) yield (dcName, getAllSchemas(HiveDcs.dcCotainer.getDc(dcName))))
      case _ => None
    }

  }

  def getAllTables(dcName: String, schemaName: String): Option[(String, List[String])] = {
    val dc = HiveDcs.dcCotainer;
    val tableNameCollector = new JdbcResultCollector[String] {
      override def collect(resultSet: ResultSet): String = resultSet.getString(1)
    }
    dc.getDc(dcName) match {
      case dc: SimpleHiveDc => Some(dcName, {
        val task = new HiveQueryTask[String, SimpleHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = "show tables"

          override def connectionDesc(): SimpleHive2JdbcConnDesc = new SimpleHive2JdbcConnDesc {
            override def user(): String = dc.getUsername

            override def password(): String = dc.getPassowrd

            override def host(): String = dc.getHost

            override def schema(): String = schemaName

            override def port(): Int = dc.getPort
          }
        }
        Executor().executeTaskSync(task)
        task.queryResults().asScala.toList
      })
      case dc: KerberizedHiveDc => Some(dcName, {
        val task = new HiveQueryTask[String, KerberizedHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = "show tables"

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = new KerberizedHive2JdbcConnDesc {

            override def principal(): String = dc.getPrincipal

            override def host(): String = dc.getHost

            override def schema(): String = schemaName

            override def port(): Int = dc.getPort
          }
        }
        Executor().executeTaskSync(task)
        task.queryResults().asScala.toList
      })
      case _ => None
    }
  }


  // default kerberized jdbc Connection description
  def getKerberizedDesc(dc: KerberizedHiveDc) = new KerberizedHive2JdbcConnDesc {
    override def principal(): String = dc.getPrincipal

    override def host(): String = dc.getHost

    override def port(): Int = dc.getPort

  }

  // default simpile jdbc Connection description
  def getSimpleDesc(dc: SimpleHiveDc) = new SimpleHive2JdbcConnDesc {
    override def user(): String = dc.getUsername

    override def password(): String = dc.getPassowrd

    override def host(): String = dc.getHost

    override def port(): Int = dc.getPort
  }

  /**
    * get table columns .
    *
    * @param dcName     data center name
    * @param schemaName schema name
    * @param tableName  table or view name
    * @return (name,type)
    */
  def getTableTile(dcName: String, schemaName: String, tableName: String): Option[List[(String, String)]] = {
    val tableTitleCollector = new JdbcResultCollector[(String, String)] {
      override def collect(resultSet: ResultSet): (String, String) = (resultSet.getString(1), resultSet.getString(2))
    }
    Some(
      HiveDcs.dcCotainer.getDc(dcName) match {
        case dc: HiveDc => {
          val task = dc match {
            case dc: SimpleHiveDc => {
              new HiveQueryTask[(String, String), SimpleHive2JdbcConnDesc] {
                override def collector(): JdbcResultCollector[(String, String)] = tableTitleCollector

                override def sql(): String = s"desc $tableName"

                override def connectionDesc(): SimpleHive2JdbcConnDesc = conSimpleDc2Desc(dc, schemaName)
              }
            }
            case dc: KerberizedHiveDc => {
              new HiveQueryTask[(String, String), KerberizedHive2JdbcConnDesc] {
                override def collector(): JdbcResultCollector[(String, String)] = tableTitleCollector

                override def sql(): String = s"desc $tableName"

                override def connectionDesc(): KerberizedHive2JdbcConnDesc = conKrbDc2Desc(dc, schemaName)
              }
            }
          }

          Executor().executeTaskSync(task)
          task.queryResults().asScala.toList
        }
        case _ => Nil
      }
    )
  }

  def getTableContents(dcName: String, schemaName: String, tableName: String, limit: Int): Option[List[List[String]]] = {

    val dataCollector = new JdbcResultCollector[List[String]] {
      override def collect(resultSet: ResultSet): List[String] = {
        (for (i <- 1 to resultSet.getMetaData.getColumnCount) yield resultSet.getString(i)).toList
      }
    }
    Some(
      HiveDcs.dcCotainer.getDc(dcName) match {
        case dc: HiveDc => {
          val task = dc match {
            case dc: SimpleHiveDc => new HiveQueryTask[List[String], SimpleHive2JdbcConnDesc] {

              override def collector(): JdbcResultCollector[List[String]] = dataCollector

              override def sql(): String = s"select * from $tableName limit $limit"

              override def connectionDesc(): SimpleHive2JdbcConnDesc = conSimpleDc2Desc(dc, schemaName)
            }
            case dc: KerberizedHiveDc => new HiveQueryTask[List[String], KerberizedHive2JdbcConnDesc] {
              override def collector(): JdbcResultCollector[List[String]] = dataCollector

              override def sql(): String = s"select * from $tableName limit $limit"

              override def connectionDesc(): KerberizedHive2JdbcConnDesc = conKrbDc2Desc(dc, schemaName)
            }
          }
          Executor().executeTaskSync(task)
          task.queryResults().asScala.toList
        }
        case _ => Nil
      }
    )
  }

  def conKrbDc2Desc(dc: KerberizedHiveDc, schemaName: String): KerberizedHive2JdbcConnDesc =
    new KerberizedHive2JdbcConnDesc {
      override def principal(): String = dc.getPrincipal

      override def host(): String = dc.getHost

      override def port(): Int = dc.getPort

      override def schema(): String = schemaName
    }

  def conSimpleDc2Desc(dc: SimpleHiveDc, schemaName: String): SimpleHive2JdbcConnDesc =
    new SimpleHive2JdbcConnDesc {
      override def user(): String = dc.getUsername

      override def password(): String = dc.getPassowrd

      override def host(): String = dc.getHost

      override def port(): Int = dc.getPort

      override def schema(): String = schemaName
    }
}

object HiveService {
  val instance = new HiveService

  def apply(): HiveService = instance;
}