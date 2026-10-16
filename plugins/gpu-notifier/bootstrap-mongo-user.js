// Copyright (c) 2026, Meshy. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Run with: mongosh --nodb --quiet --file plugins/gpu-notifier/bootstrap-mongo-user.js
// Credentials come from the environment; the worker never receives admin access.
// This script targets the existing, self-managed Xinjiang MongoDB deployment.
(() => {
  let step = "validate environment";
  try {
    const database = process.env.GPU_NOTIFY_MONGO_DATABASE;
    const adminUri = process.env.GPU_NOTIFY_MONGO_ADMIN_URI;
    const mechanism = process.env.GPU_NOTIFY_MONGO_AUTH_MECHANISM || "x509";
    const username = process.env.GPU_NOTIFY_MONGO_USERNAME ||
      (mechanism === "x509" ? "CN=gpu-notifier,OU=GPU-Notifications,O=Meshy" : "gpu-notifier");
    const password = process.env.GPU_NOTIFY_MONGO_PASSWORD;
    const collection = "gpu_notification_state";
    const roleName = "gpu_notifier_state_writer";
    const actions = ["find", "insert", "remove", "update"];
    const writeConcern = { w: "majority", wtimeout: 10000 };
    if (!database || /[\/\\. "$*<>:|?\u0000]/.test(database) ||
        ["admin", "local", "config"].includes(database)) {
      throw new Error("explicit application database required");
    }
    if (!adminUri || !username || username.includes("\u0000") ||
        !["x509", "scram"].includes(mechanism)) {
      throw new Error("administrator URI and valid username required");
    }

    step = "connect and inspect existing grants";
    const connection = new Mongo(adminUri);
    const target = connection.getDB(database);
    const accounts = connection.getDB(mechanism === "x509" ? "$external" : database);
    const existingRole = target.getRole(roleName, { showPrivileges: true });
    const existingUser = accounts.getUser(username);
    const collections = target.getCollectionInfos({ name: collection });
    const privilege = { resource: { db: database, collection }, actions };

    // Check everything before making changes. An existing broader account needs
    // separate review; provisioning must not silently edit another account.
    step = "check existing role is collection-scoped";
    if (existingRole) {
      const privileges = existingRole.privileges || [];
      const grant = privileges[0];
      if ((existingRole.roles || []).length !== 0 || privileges.length !== 1 ||
          grant.resource?.db !== database || grant.resource?.collection !== collection ||
          Object.keys(grant.resource).length !== 2 ||
          JSON.stringify([...grant.actions].sort()) !== JSON.stringify(actions)) {
        throw new Error("existing role differs from notifier contract");
      }
    }
    step = "check existing user has only the notifier role";
    if (existingUser) {
      const roles = existingUser.roles || [];
      if (roles.length !== 1 || roles[0].role !== roleName || roles[0].db !== database) {
        throw new Error("existing account has different roles");
      }
    } else if (mechanism === "scram" && !password) {
      throw new Error("password required for initial account creation");
    }
    step = "check collection type";
    if (collections.length && (collections[0].type !== "collection" || collections[0].options?.capped)) {
      throw new Error("notification state requires a normal collection");
    }

    step = "create notification collection";
    if (!collections.length) {
      const created = target.runCommand({ create: collection, writeConcern });
      if (created.ok !== 1) throw new Error("collection creation failed");
    }
    step = "create collection-scoped role";
    if (!existingRole) {
      target.createRole({ role: roleName, privileges: [privilege], roles: [] }, writeConcern);
    }
    step = "create notifier account";
    if (!existingUser) {
      const user = {
        user: username,
        roles: [{ role: roleName, db: database }],
        customData: { managedBy: "gpu-notifier-bootstrap" },
      };
      if (mechanism === "scram") user.pwd = password;
      accounts.createUser(user, writeConcern);
    }
    print(existingUser
      ? "Notifier account and collection verified; existing credentials preserved."
      : "Notifier account and collection created with collection-scoped access.");
  } catch (error) {
    // A driver exception may contain a URI. Log only the operation and code.
    const code = Number.isInteger(error.code) ? ` (MongoDB code ${error.code})` : "";
    print(`Notifier provisioning failed during: ${step}${code}.`);
    quit(1);
  }
})();
