# Changelog

## 0.25.0 (2026-10-27)

### Features

- **agentspec**: add tagged_only so hosts can keep an agent out of untagged messages (#46) ([eb7a9db](https://github.com/urmzd/fiber/commit/eb7a9db3c56f7ab6b1b3c34424a8dbe3b03685f9))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.24.1...v0.25.0)


## 0.24.1 (2026-10-27)

### Performance

- **bus**: wake only the Postgres readers a publish concerns (#45) ([b6ec42a](https://github.com/urmzd/fiber/commit/b6ec42aefc91e4d9775ebaf69aaf59fea228fa7f))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.24.0...v0.24.1)


## 0.24.0 (2026-10-22)

### Features

- **envelope**: a meta key starting with _ is the host's and is never sent to the model (#43) ([20d3a27](https://github.com/urmzd/fiber/commit/20d3a27bd75070773e9d665b34e8d93f1ff7dd76))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.23.0...v0.24.0)


## 0.23.0 (2026-10-22)

### Features

- **github**: raise a comment addressed to the app for its default agent (#42) ([804e2f5](https://github.com/urmzd/fiber/commit/804e2f5909a21fd7e7c88bef5070fbc33ee7352a))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.22.1...v0.23.0)


## 0.22.1 (2026-10-22)

### Bug Fixes

- **deps**: anchor v0.5.1, so a stop mid-claim never leases a run to nobody (#41) ([1132e20](https://github.com/urmzd/fiber/commit/1132e207d9528f79623404a26c7e0b4568e385a4))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.22.0...v0.22.1)


## 0.22.0 (2026-10-22)

### Features

- per-person tool credentials, and fiber run for local tests (#40) ([b588324](https://github.com/urmzd/fiber/commit/b588324b836e2550627b28de8b6250726ce3f2ac))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.21.0...v0.22.0)


## 0.21.0 (2026-10-21)

### Breaking

- fiber is a library composed in code; no binary, no server, no mandatum.v1 (#39) ([ea8ca50](https://github.com/urmzd/fiber/commit/ea8ca501f1d061cb36d00ac97c332a17c300ca12))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.20.1...v0.21.0)


## 0.20.1 (2026-10-21)

### Bug Fixes

- **mcptools**: inline local $defs references before converting a tool schema (#38) ([66b9f81](https://github.com/urmzd/fiber/commit/66b9f81753fddc19060065186ecd91a6797b2385))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.20.0...v0.20.1)


## 0.20.0 (2026-10-20)

### Features

- **fiberapp**: public composition with extension points and a providers file (#36) ([6045747](https://github.com/urmzd/fiber/commit/6045747758eb6ea9c4a9f5a92749ad8352b9ea32))

### Bug Fixes

- **agentrt**: drop a tool group that cannot be expanded, with a warning (#37) ([e9e07c0](https://github.com/urmzd/fiber/commit/e9e07c0a8b919577902740d04c1d9d212a050499))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.19.0...v0.20.0)


## 0.19.0 (2026-10-20)

### Features

- **mcptools**: expand mcp:<provider>[:write] grants from the tool provider store (#35) ([55b362a](https://github.com/urmzd/fiber/commit/55b362aa4107f814208cabeaf146f9704169ca21))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.18.0...v0.19.0)


## 0.18.0 (2026-10-20)

### Features

- **envelope**: carry the host's user id on a tag's actor (#34) ([5c5fc31](https://github.com/urmzd/fiber/commit/5c5fc315c6be07facb4ab7df191f84ee45edb681))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.17.1...v0.18.0)


## 0.17.0 (2026-10-20)

### Features

- model fallbacks with fallback conditions, and per-call approvals (#32) ([0d43e9b](https://github.com/urmzd/fiber/commit/0d43e9b72b0b6e948ce81681df7664e381b93db2))

### Bug Fixes

- **runtime**: forward per-call approval through the host call tool (#33) ([aa7cc4c](https://github.com/urmzd/fiber/commit/aa7cc4c0105f67ebb981511d6ee8e60546edf21b))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.16.0...v0.17.0)


## 0.16.0 (2026-10-20)

### Features

- **agentrt**: OpenAI-compatible endpoints and MCP tool groups (#31) ([a108412](https://github.com/urmzd/fiber/commit/a108412281f8b769f7e8e5405d89ee131ecb4b6f))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.15.0...v0.16.0)


## 0.15.0 (2026-10-20)

### Features

- host-triggered compaction and tool arguments on tool events (#23) ([c8c1d6d](https://github.com/urmzd/fiber/commit/c8c1d6d7c7c19a3939d82e40b1d13857b48566b6))

### Misc

- **runtime**: prove an inline sub-task's workspace and spawn confinement (#22) ([52b5e2a](https://github.com/urmzd/fiber/commit/52b5e2ae571e3b800547749d21e713fe4dfc975b))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.14.2...v0.15.0)


## 0.14.2 (2026-10-20)

### Bug Fixes

- **mcptools**: confine the credential to its origin and harden tool schemas (#25) ([05018fd](https://github.com/urmzd/fiber/commit/05018fd418af3a8c3575a7136048b2d7d979b278))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.14.1...v0.14.2)


## 0.14.1 (2026-10-18)

### Bug Fixes

- **runtime**: run in-turn sub-tasks on the delegating turn's node (#20) ([d3438db](https://github.com/urmzd/fiber/commit/d3438dbc0bf829e8d7f06f0554087b4be4d65327))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.14.0...v0.14.1)


## 0.14.0 (2026-10-18)

### Features

- add MCP tool providers as versioned records (#21) ([1fa1854](https://github.com/urmzd/fiber/commit/1fa1854c8482e677ba46bc46de9da8a751fe4055))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.13.0...v0.14.0)


## 0.13.0 (2026-10-18)

### Breaking

- rename mandatum to fiber (#19) ([78aa579](https://github.com/urmzd/fiber/commit/78aa5799001388fb2226f9c0ef14c25ad758f1eb))

### Misc

- **deps**: upgrade saige to v0.37.0 (#18) ([97e11d3](https://github.com/urmzd/fiber/commit/97e11d3480368d539868cbd5394550438d486362))

[Full Changelog](https://github.com/urmzd/fiber/compare/v0.12.0...v0.13.0)


## 0.12.0 (2026-10-16)

### Features

- support host-constructed helper invocations (#17) ([799b78e](https://github.com/urmzd/mandatum/commit/799b78eb12773400f08323034e9af2c754197124))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.11.0...v0.12.0)


## 0.11.0 (2026-10-14)

### Breaking

- park a run on a person and resume it with the answer (#16) ([8aaec82](https://github.com/urmzd/mandatum/commit/8aaec82b8fa753d3438258f60da17b94629cc09f))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.10.0...v0.11.0)


## 0.10.0 (2026-10-14)

### Features

- report usage per run (#15) ([2a1db37](https://github.com/urmzd/mandatum/commit/2a1db3783c89a52aff1f9f43d046498420e338bc))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.9.0...v0.10.0)


## 0.9.0 (2026-10-14)

### Features

- **cli**: reconcile threads through their connectors and bring files in from Slack, GitHub and Jira ([9b45449](https://github.com/urmzd/mandatum/commit/9b454490ad91c2b1b127c8cead30fd1a030a153c))
- **jira**: bring issue attachments in through the site's content endpoint ([3bda4e4](https://github.com/urmzd/mandatum/commit/3bda4e49c8d023be23c5ac55fe1bc08471108364))
- **github**: bring attached images and videos in as attachments and list conversations for reconciliation ([51fb1a8](https://github.com/urmzd/mandatum/commit/51fb1a812fd3a20a7d17edfd904055009689a8e1))
- **slack**: bring message files in as attachments and list threads for reconciliation ([d23632e](https://github.com/urmzd/mandatum/commit/d23632e4dd0a2db3b5e131c2bd9a0f0eaeb21274))
- **attachment**: recognise voice clips in audio-only WebM and M4A containers ([a675aea](https://github.com/urmzd/mandatum/commit/a675aea9600dcc4c58a6d543c0feeab08a022de0))
- **intake**: collect a message's files under per-tag caps without failing the tag ([61b039e](https://github.com/urmzd/mandatum/commit/61b039e4dcb38b1fc6f440adfee8e8c2738c6adc))
- **runtime**: journal a request's files and replay them byte-stably in later turns ([3b21ba2](https://github.com/urmzd/mandatum/commit/3b21ba2520fe4ed2462f68fa546a1574f5d52bf4))
- **thread**: journal attachments with each entry and reconcile surface files once ([c6d6e97](https://github.com/urmzd/mandatum/commit/c6d6e97b5d85fb98f2c2f6e8b1661e46c82a7f63))
- **attachment**: reference a file that could not be fetched so the agent is told ([1eb1ca5](https://github.com/urmzd/mandatum/commit/1eb1ca5fd4c865e54cb0db9dacfbb9cb6984b490))

### Misc

- **slack**: merge declaration flagged by staticcheck ([924bba0](https://github.com/urmzd/mandatum/commit/924bba0aa75c16a27730ec57ec14db1a859ad8ef))
- **github**: gofmt attachment tests ([2fbda0f](https://github.com/urmzd/mandatum/commit/2fbda0f6f3e4342ec57927b4c9df24d72e3dc9d6))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.8.0...v0.9.0)


## 0.8.0 (2026-10-14)

### Features

- **runtime**: stop a run mid-turn and keep its partial answer (#13) ([662e2d3](https://github.com/urmzd/mandatum/commit/662e2d3253488972bf56c96f2dbfb83b32f6eb61))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.7.0...v0.8.0)


## 0.7.0 (2026-10-14)

### Breaking

- **runtime**: scope a run's identity to its tenant ([3a4ad50](https://github.com/urmzd/mandatum/commit/3a4ad507137532d818e047f09348bd79e704546b))
- **server**: accept file uploads and take a tag's actor from the credential ([45fa707](https://github.com/urmzd/mandatum/commit/45fa707eec021e32e1fc85f1626dbf74ffad5e3a))
- **registry**: add a Postgres store and an atomic expected-revision delete ([7d345e4](https://github.com/urmzd/mandatum/commit/7d345e4eca353daec9546585401345c209531b30))

### Features

- **cli**: run on Postgres and wire knowledge, threads, files and the author ([16e2109](https://github.com/urmzd/mandatum/commit/16e210930f08d2a561f3bc0a21de26432d959460))
- **author**: add the registry as agent tools and a built-in author agent ([25620c7](https://github.com/urmzd/mandatum/commit/25620c7e7ac447771fcf13e9441798be3ea57b71))
- **runtime**: pin thread history at accept and ground runs in knowledge ([0d4f383](https://github.com/urmzd/mandatum/commit/0d4f3833eee7f71bb9c034d8e869d4471380b004))
- **agentrt**: add compaction, prompt caching, history and attachments ([fea5d5e](https://github.com/urmzd/mandatum/commit/fea5d5e339dc256e54280d6478ce09660d507533))
- **envelope**: carry attachments on a tag ([bb01994](https://github.com/urmzd/mandatum/commit/bb01994e0f48723c46b98f456156de890e9e2e11))
- **thread**: add a per-thread conversation journal ([fcc0852](https://github.com/urmzd/mandatum/commit/fcc085255775ab500ccd5c227ce9cd47b58249b8))
- **attachment**: add a content-addressed blob store and file resolution ([325d43f](https://github.com/urmzd/mandatum/commit/325d43fa7619a8b471cf32c869aa60a17b9e23af))
- **knowledge**: add hybrid retrieval with citations ([8dc8632](https://github.com/urmzd/mandatum/commit/8dc8632010890c481dc66efed22857f416365c6b))
- **durable**: own the Postgres schema for duraturo's ledger and queue ([88462f1](https://github.com/urmzd/mandatum/commit/88462f19bc017ecdd0b42ddceed0b2e615f91340))

### Bug Fixes

- **server**: preserve SSE slow-consumer reconnect hints ([9ead27b](https://github.com/urmzd/mandatum/commit/9ead27bf7bca6575dda03a0288896be07e4bea0a))
- **server**: always announce draining before ending an SSE stream ([af472cb](https://github.com/urmzd/mandatum/commit/af472cb9f3e91164af70d97bb57851bffc478e0b))

### Misc

- run the Postgres and Redis backends in an integration job ([09a3efb](https://github.com/urmzd/mandatum/commit/09a3efb335892489982abb73f021eae603b77fb9))
- add a Dockerfile and a compose stack with an end-to-end check ([1e3476e](https://github.com/urmzd/mandatum/commit/1e3476ef7d7650c3346968b855127bef3a373d12))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.6.0...v0.7.0)


## 0.6.0 (2026-10-14)

### Breaking

- **connectors**: separate policy and bind Slack ingress to teams (#11) ([612a002](https://github.com/urmzd/mandatum/commit/612a0027ac7ce73407d5e95ce8847ce322ae7f42))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.5.0...v0.6.0)


## 0.5.0 (2026-10-14)

### Features

- **github**: add structured reviews and PR collaboration ([8703695](https://github.com/urmzd/mandatum/commit/8703695d17a61b405acccdc41b493319765836a5))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.4.0...v0.5.0)


## 0.4.0 (2026-10-10)

### Breaking

- depend on legatus, the renamed dispatch (#8) ([61ff1f1](https://github.com/urmzd/mandatum/commit/61ff1f1aa4dba9ed04e29901d49d1c5fd65a36ca))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.3.0...v0.4.0)


## 0.3.0 (2026-10-10)

### Breaking

- rename the project from opentag to mandatum (#7) ([92e7d1b](https://github.com/urmzd/mandatum/commit/92e7d1b5e0275abcef0779dc755a39e966379173))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.2.0...v0.3.0)


## 0.2.0 (2026-10-10)

### Features

- **runtime**: let a host carry out a delegation for a turn that is a run (#6) ([f8d8fda](https://github.com/urmzd/mandatum/commit/f8d8fda4a91831e5f6595fc9f456d724aee8c174))

### Misc

- **deps**: bump saige to v0.34.0 (#5) ([f4be729](https://github.com/urmzd/mandatum/commit/f4be729ab035a22755b2c8e9256c5250a9b53872))
- **release**: read the sr-releaser key from the release environment (#4) ([0d66d8f](https://github.com/urmzd/mandatum/commit/0d66d8f70c16a5424c61d6c45e7f5244589aa8e4))
- **deps**: bump saige to v0.24.0 (#3) ([e6cb2ac](https://github.com/urmzd/mandatum/commit/e6cb2acf04073e84cb17acf467dcf0b1b6f16624))
- upgrade urmzd/sr action to v9 (#2) ([d16c081](https://github.com/urmzd/mandatum/commit/d16c08121bac6ac35b91d33972bad917b6ec98ef))

[Full Changelog](https://github.com/urmzd/mandatum/compare/v0.1.0...v0.2.0)


## 0.1.0 (2026-10-09)

### Features

- opentag, an agent platform and pub/sub bus for agents ([946ab53](https://github.com/urmzd/mandatum/commit/946ab53d59c5314bfff7f6ca24c924882868c6ea))

### Bug Fixes

- remove a lost wakeup in the retry test and repair CI ([f77c845](https://github.com/urmzd/mandatum/commit/f77c845cff664d8cb2319d34b41e2e2e6317fa91))

### Misc

- update model examples and bump saige to v0.22.0 (#1) ([5a43167](https://github.com/urmzd/mandatum/commit/5a4316754d95c652ff3610b0d805339085a11ad7))


All notable changes to this project are documented here.
Entries are generated from conventional commits by [sr](https://github.com/urmzd/sr).
