# Local Launch

Start a local bootstrap node:

```powershell
npm run start:bootstrap
```

Start three service nodes in separate terminals:

```powershell
npm run start:node -- examples/local/node-1.json
npm run start:node -- examples/local/node-2.json
npm run start:node -- examples/local/node-3.json
```

Those three files are the current bootstrap-voter example. They still use
`compatibilityMode: "legacy-static-membership"` for the initial voter set.
That is intentional until explicit `initCluster` bootstrap lands.

Join a fourth node without editing the existing voter configs:

```powershell
npm run start:node -- examples/local/joiner.json
```

The joiner starts as a learner. It discovers the cluster from the shared
`clusterSecret`, derives its transport and join identities from
`clusterSecret + machineIdentity`, catches up for reads, and must be promoted
through committed membership before it can vote or satisfy durability.

Write through any node:

```powershell
curl -X PUT "http://127.0.0.1:3001/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer writer" `
  -H "content-type: application/json" `
  -d "{\"value\":{\"hello\":\"world\"}}"
```

Read from another node:

```powershell
curl "http://127.0.0.1:3002/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer reader"
```

History:

```powershell
curl "http://127.0.0.1:3003/kv/hash:abc/history?keyspace=default" `
  -H "authorization: Bearer reader"
```

Delete:

```powershell
curl -X DELETE "http://127.0.0.1:3002/kv/hash:abc?keyspace=default" `
  -H "authorization: Bearer writer"
```

The public write surface is witness-first. Healthy witness nodes accept CRUD
and forward to the current leader. Direct leader writes may be refused with
structured hints that point clients back to witness entrypoints.

Status:

```powershell
curl "http://127.0.0.1:3001/status/replication"
curl "http://127.0.0.1:3001/status/writers"
curl "http://127.0.0.1:3001/status/leader"
```

Export a snapshot:

```powershell
npm run snapshot -- export http://127.0.0.1:3001 admin .\tmp\snapshot.json
```

Import a snapshot:

```powershell
npm run snapshot -- import http://127.0.0.1:3001 admin .\tmp\snapshot.json
```

Rotate the active encryption key:

```powershell
curl -X POST "http://127.0.0.1:3001/admin/encryption/rotate" `
  -H "authorization: Bearer admin" `
  -H "content-type: application/json" `
  -d "{\"keyId\":\"next\"}"
```
