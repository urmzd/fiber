# Changelog

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
