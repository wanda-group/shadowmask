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

package org.shadowmask.web.common.user

import java.util.ResourceBundle

import authentikat.jwt.{JsonWebToken, JwtClaimsSet, JwtHeader}
import org.apache.directory.api.ldap.model.entry.Entry
import org.apache.directory.api.ldap.model.message.SearchScope
import org.apache.directory.ldap.client.api.{LdapConnection, LdapNetworkConnection}
import org.shadowmask.web.utils.MD5Hasher

/**
  * abstract parent class
  */
abstract class UserAuth {
  def auth(user: Option[User]): Option[Token]

  def verify(token: Option[Token]): Option[User]
}

/**
  * token
  *
  * @param token
  */
case class Token(token: String)

/**
  * user
  *
  * @param username
  * @param password
  */
case class User(username: String, password: String)

/*----------------------------------------------------------------
  Auth strategy
 ----------------------------------------------------------------*/


/**
  *
  */
class PlainUserAuth private extends UserAuth {
  /**
    * auth one user and return a token
    *
    * @param user
    * @return
    */
  override def auth(user: Option[User]): Option[Token] = if (user != None && ConfiguredUsers().verify(user.get.username, user.get.password)) {
    Some(Token(JsonWebToken(JwtHeader("HS256"), JwtClaimsSet(Map("username" -> user.get.username)), ConfiguredUsers().secret)))
  } else None

  /**
    * verify a user with a token
    *
    * @param token
    * @return
    */
  override def verify(token: Option[Token]): Option[User] = {
    if (JsonWebToken.validate(token.get.token, ConfiguredUsers().secret)) {
      token.getOrElse(Token("")).token match {
        case JsonWebToken(header, value, key) => {
          Option(User(value.asSimpleMap.get.get("username").getOrElse(""), ""))
        }
        case _ => None
      }
    } else None

  }
}

object PlainUserAuth {
  val instance = new PlainUserAuth

  def apply(): PlainUserAuth = instance
}


class LdapServerAuth private extends UserAuth {
  override def auth(user: Option[User]): Option[Token] = {
    user match {
      case None => None
      case Some(user) =>
        LdapToolKit.authUser(Some(user.username), Some(user.password)) match {
          case Some(true) =>
            Some(Token(JsonWebToken(JwtHeader("HS256"), JwtClaimsSet(Map("username" -> user.username)), user.password)))
          case _ => None
        }
    }
  }

  override def verify(token: Option[Token]): Option[User] = {
    if (JsonWebToken.validate(token.get.token, ConfiguredUsers().secret)) {
      token.getOrElse(Token("")).token match {
        case JsonWebToken(header, value, key) => {
          Option(User(value.asSimpleMap.get.get("username").getOrElse(""), ""))
        }
        case _ => None
      }
    } else None
  }
}

object LdapServerAuth {
  val instance = new LdapServerAuth

  def apply(): LdapServerAuth = instance
}

//mocked ldap auth
class MockLdapServerAuth private extends UserAuth {
  override def auth(user: Option[User]): Option[Token] = PlainUserAuth().auth(user)

  override def verify(token: Option[Token]): Option[User] = PlainUserAuth().verify(token)
}

object MockLdapServerAuth {
  val instance = new MockLdapServerAuth

  def apply(): MockLdapServerAuth = instance
}

/*----------------------------------------------------------------
  Auth strategy
 ----------------------------------------------------------------*/


/**
  * user auth provider
  */
trait AuthProvider {
  def getAuth: UserAuth
}

trait PlainAuthProvider extends AuthProvider {
  def getAuth() = PlainUserAuth()
}

trait ConfiguredAuthProvider extends AuthProvider {
  def getAuth() = {
    ShadowmaskProp().authType match {
      case "ldap" => LdapServerAuth()
      case "plain" => PlainUserAuth()
    }
  }
}


/**
  * users configured in admin.properties
  */
class ConfiguredUsers private {
  val (userMap, forbiddenUsers, secret) = {
    val resource = ResourceBundle.getBundle("admin")
    var usermap = Map[String, String]()
    var forbiddenSet = Set[String]()
    resource.getString("admin.users").split(";").foreach((s) => {
      val arr = s.split(":")
      usermap += (arr(0) -> arr(1))
    })
    resource.getString("admin.users.forbidden").split(";").foreach(s => forbiddenSet += s)
    (usermap, forbiddenSet, resource.getString("admin.users.secret"))
  }

  def verify(username: String, password: String): Boolean = username != null && password != null &&
    userMap.keySet.contains(username) && !forbiddenUsers.contains(username) && {
    val pwd = userMap.get(username).getOrElse("")
    pwd.equals(MD5Hasher().hashs2s(password))
  }
}

object ConfiguredUsers {
  val instance = new ConfiguredUsers

  def apply() = instance
}

///////////ldap properties ////
class LdapProp private(
                        val host: String,
                        val port: Int,
                        val manager: String,
                        val managerPwd: String,
                        val rootDir: String,
                        val userDomain: String,
                        val usernameTemplete: String
                      )

object LdapProp {
  val resource = ResourceBundle.getBundle("ldap")
  val instance = new LdapProp({
    resource.getString("user.auth.ldap.host")
  }, {
    resource.getString("user.auth.ldap.port").toInt
  }, {
    resource.getString("user.auth.ldap.manager.dn")
  }, {
    resource.getString("user.auth.ldap.manager.password")
  }, {
    resource.getString("user.auth.ldap.rootdir")
  }, {
    resource.getString("user.auth.ldap.users.domain")
  }, {
    resource.getString("user.auth.ldap.users.nametemplete")
  })

  def apply(): LdapProp = instance
}

/////////////shadow mask properties /////////////
class ShadowmaskProp private(val authType: String)

object ShadowmaskProp {
  val instance = new ShadowmaskProp({
    val resource = ResourceBundle.getBundle("shadowmask")
    resource.getString("user.auth.method")
  })

  def apply(): ShadowmaskProp = instance
}

class LdapUser(
                val dn:String,
                val uid:String
              ){
  def this(entry: Entry) = this(entry.getDn.getName,entry.get("uid").getString)
}


//ldap toolkit
object LdapToolKit {
  val ldapProp = LdapProp();

  import ldapProp._


  /**
    * user authorization by ldap server .
    *
    * @param name     usename
    * @param password password of user .
    * @return
    */
  def authUser(name: Option[String], password: Option[String]): Option[Boolean] = {
    accessLdap[Some[Boolean]]((conn) => {
      conn.bind(usernameTemplete.replace("{username}", name.get) + "," + userDomain, password.get);
      Some(true)
    }, (conn, e) => {
      Some(false)
    })
  }

  def fetchLdapUserList():Option[List[LdapUser]] = {
    Some(
      accessLdap[List[LdapUser]](conn=>{
        conn.bind(manager,managerPwd)
        val cursor = conn.search(userDomain,"(objectclass=*)",SearchScope.SUBTREE)
        var list:List[LdapUser] = Nil
        while (cursor.next()){
          list = new LdapUser(cursor.get()) :: list
        }
        list
      },(conn,e)=>{
        Nil
      })
    )
  }

  /**
    * access ldap server
    *
    * @param cmd    code run normally
    * @param except code will after exception occured .
    * @tparam T result type
    * @return
    */
  def accessLdap[T](cmd: (LdapConnection) => T, except: (LdapConnection, Exception) => T) = {
    var connection: LdapConnection = null;
    try {
      connection = new LdapNetworkConnection(LdapProp().host, LdapProp().port);
      cmd(connection)
    } catch {
      case e: Exception => except(connection, e)
    } finally {
      if (null != connection) {
        connection.close()
      }
    }

  }
}




