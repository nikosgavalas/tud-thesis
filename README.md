#### Usage:

`./kvstore`

#### Tests:

`python -m unittest`

#### Roadmap:

- [x] memtable
- [x] bloom filters
- [x] flush the memtable
- [x] file format
  - [x] data
  - [x] fence pointers persistence
  - [x] bloom filters persistence
- [x] merging multiple levels
- [ ] WAL
- [ ] asyncio
