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
