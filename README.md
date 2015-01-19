# mongo-pyload

This script bulk inserts empty documents into MongoDB to test its concurrency limits.

### Usage and Options

All configurable options have reasonable default values. Run with:
```
python pyload.py
```

`--number` the number of total documents to insert. <br>
`--batchsize` the number of documents in a single bulk insert batch <br>
`--database` the database to insert into (default: `pyload`) <br>
`--collection` the collection or collection prefix (when using multiple collections) (default: `coll`) <br>
`--host` host where MongoDB is located <br>
`--port` port to connect to <br>
`--write-concern` the write concern to use. (default: `0`, which is `{w: 0}`) <br>
`--round-robin` the number of collections to write to in round-robin fashion (default: `10`) <br>
`--threads` the number of threads (actually, processes) to run simultaneously (default: `#cores`) <br>

