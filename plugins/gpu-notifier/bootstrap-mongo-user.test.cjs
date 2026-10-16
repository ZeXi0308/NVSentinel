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

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = fs.readFileSync(path.join(__dirname, 'bootstrap-mongo-user.js'), 'utf8');
const database = 'HealthEventsDatabase';
const roleName = 'gpu_notifier_state_writer';
const role = {
  role: roleName,
  roles: [],
  privileges: [{
    resource: { db: database, collection: 'gpu_notification_state' },
    actions: ['find', 'insert', 'update', 'remove'],
  }],
};

function provision(options = {}) {
  const writes = [], output = [];
  let exitCode = 0;
  const db = name => ({
    getRole: () => options.role || null,
    getUser: () => options.user || null,
    getCollectionInfos: () => options.exists ? [{ type: 'collection', options: {} }] : [],
    runCommand: value => { writes.push({ name, kind: 'collection', value }); return { ok: 1 }; },
    createRole: value => writes.push({ name, kind: 'role', value }),
    createUser: value => writes.push({ name, kind: 'user', value }),
  });
  const context = {
    process: { env: {
      GPU_NOTIFY_MONGO_DATABASE: database,
      GPU_NOTIFY_MONGO_ADMIN_URI: 'mongodb://admin:private-password@example.invalid',
      ...options.env,
    } },
    Mongo: function () {
      if (options.connectionError) throw new Error('mongodb://admin:private-password@example.invalid');
      return { getDB: db };
    },
    print: message => output.push(message),
    quit: code => { exitCode = code; },
  };
  vm.runInNewContext(script, context);
  return { writes: JSON.parse(JSON.stringify(writes)), output, exitCode };
}

test('X509 account lives in $external; grants touch one application collection', () => {
  const result = provision();
  assert.equal(result.exitCode, 0);
  assert.deepEqual(result.writes.map(w => [w.name, w.kind]), [
    [database, 'collection'], [database, 'role'], ['$external', 'user'],
  ]);
  assert.deepEqual(result.writes[1].value.privileges, [{
    resource: { db: database, collection: 'gpu_notification_state' },
    actions: ['find', 'insert', 'remove', 'update'],
  }]);
  assert.equal('pwd' in result.writes[2].value, false);
});

test('rerun preserves an existing account and credentials', () => {
  const result = provision({ role, user: { roles: [{ role: roleName, db: database }] }, exists: true });
  assert.equal(result.exitCode, 0);
  assert.deepEqual(result.writes, []);
});

test('broader existing role stops before any mutation', () => {
  const broad = structuredClone(role);
  broad.privileges[0].resource.collection = '';
  const result = provision({ role: broad });
  assert.equal(result.exitCode, 1);
  assert.deepEqual(result.writes, []);
});

test('existing account with another role stops before any mutation', () => {
  const result = provision({ user: { roles: [{ role: 'readWrite', db: database }] } });
  assert.equal(result.exitCode, 1);
  assert.deepEqual(result.writes, []);
});

test('target database is explicit and excludes system databases', () => {
  for (const name of ['', 'admin', 'local', 'config']) {
    const result = provision({ env: { GPU_NOTIFY_MONGO_DATABASE: name } });
    assert.equal(result.exitCode, 1);
    assert.deepEqual(result.writes, []);
  }
});

test('SCRAM password only used to create new database-local user', () => {
  const env = { GPU_NOTIFY_MONGO_AUTH_MECHANISM: 'scram', GPU_NOTIFY_MONGO_PASSWORD: 'new-password' };
  const initial = provision({ env });
  assert.equal(initial.exitCode, 0);
  assert.equal(initial.writes[2].name, database);
  assert.equal(initial.writes[2].value.pwd, 'new-password');
  const repeated = provision({ env, role, user: { roles: [{ role: roleName, db: database }] }, exists: true });
  assert.deepEqual(repeated.writes, []);
});

test('SCRAM without initial password does not create partial resources', () => {
  const result = provision({ env: { GPU_NOTIFY_MONGO_AUTH_MECHANISM: 'scram' } });
  assert.equal(result.exitCode, 1);
  assert.deepEqual(result.writes, []);
});

test('connection errors do not print credentials or connection strings', () => {
  const result = provision({ connectionError: true });
  assert.equal(result.exitCode, 1);
  assert.equal(result.output.join('').includes('private-password'), false);
  assert.equal(result.output.join('').includes('mongodb://'), false);
});
