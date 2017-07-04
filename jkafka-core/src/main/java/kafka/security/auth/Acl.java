/**
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

package kafka.security.auth;

import kafka.utils.Json;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

object Acl {
  val KafkaPrincipal WildCardPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
  val String WildCardHost = "*";
  val AllowAllAcl = new Acl(WildCardPrincipal, Allow, WildCardHost, All);
  val PrincipalKey = "principal";
  val PermissionTypeKey = "permissionType";
  val OperationKey = "operation";
  val HostsKey = "host";
  val VersionKey = "version";
  val CurrentVersion = 1;
  val AclsKey = "acls";

  /**
   *
   * @param aclJson
   *
   * <p>
      {
        "version": 1,
        "acls": [
          {
            "host":"host1",
            "permissionType": "Deny",
            "operation": "Read",
            "principal": "alice User"
          }
        ]
      }
   * </p>
   *
   * @return
   */
  public void  fromJson(String aclJson): Set<Acl> = {
    if (aclJson == null || aclJson.isEmpty)
      return collection.immutable.Set.empty<Acl>;

    var collection acls.mutable.HashSet<Acl> = new collection.mutable.HashSet<Acl>();
    Json.parseFull(aclJson) match {
      case Some(m) =>
        val aclMap = m.asInstanceOf<Map[String, Any]>;
        //the acl json version.;
        require(aclMap(VersionKey) == CurrentVersion);
        val List aclSet<Map[String, Any]> = aclMap(AclsKey).asInstanceOf<List[Map[String, Any]]>;
        aclSet.foreach(item => {
          val KafkaPrincipal principal = KafkaPrincipal.fromString(item(PrincipalKey).asInstanceOf<String>);
          val PermissionType permissionType = PermissionType.fromString(item(PermissionTypeKey).asInstanceOf<String>);
          val Operation operation = Operation.fromString(item(OperationKey).asInstanceOf<String>);
          val String host = item(HostsKey).asInstanceOf<String>;
          acls += new Acl(principal, permissionType, host, operation);
        });
      case None =>
    }
    acls.toSet;
  }

  public void  toJsonCompatibleMap(Set acls<Acl>): Map<String, Any> = {
    Map(Acl.VersionKey -> Acl.CurrentVersion, Acl.AclsKey -> acls.map(acl => acl.toMap).toList);
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operation O1 from hosts H1.
 * </pre>
 * @param principal A value of *:* indicates all users.
 * @param permissionType
 * @param host A value of * indicates all hosts.
 * @param operation A value of ALL indicates all operations.
 */
case class Acl(KafkaPrincipal principal, PermissionType permissionType, String host, Operation operation) {

  /**
   * Ideally TODO we would have a symmetric toJson method but our current json library can not jsonify/dejsonify complex objects.
   * @return Map representation of the Acl.
   */
  public void  toMap(): Map<String, Any> = {
    Map(Acl.PrincipalKey -> principal.toString,
      Acl.PermissionTypeKey -> permissionType.name,
      Acl.OperationKey -> operation.name,
      Acl.HostsKey -> host);
  }

  override public void  String toString = {
    String.format("%s has %s permission for operations: %s from hosts: %s",principal, permissionType.name, operation, host)
  }

}

