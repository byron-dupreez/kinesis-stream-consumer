## Changes

### 2.1.9
- Updated dependencies to synchronise with changes & fixes in version 2.1.9 of `aws-stream-consumer-core`

### 2.1.6
- Updated dependencies

### 2.1.5
- Updated dependencies

### 2.1.4
- Updated dependencies

### 2.1.3
- Updated dependencies

### 2.1.2
- Added `.npmignore`
- Renamed `release_notes.md` to `CHANGES.md`
- Updated dependencies

### 2.1.1
- Updated `aws-core-utils` dependency to version 7.2.0
- Updated `aws-stream-consumer-core` dependency to version 2.1.1

### 2.1.0
- Updated `aws-core-utils` dependency to version 7.1.1
- Updated `aws-stream-consumer-core` dependency to version 2.1.0

### 2.0.3
- Updated `aws-core-utils` dependency to version 7.0.12
- Updated `aws-stream-consumer-core` dependency to version 2.0.6
- Updated `core-functions` dependency to version 3.0.22
- Updated `logging-utils` dependency to version 4.0.22
- Updated `task-utils` dependency to version 7.0.4
- Updated `aws-sdk` dev dependency to version 2.190.0

### 2.0.2
- Updated `aws-stream-consumer-core` dependency to version 2.0.5
- Minor fixes to JsDoc comments

### 2.0.1
- Updated `aws-core-utils` dependency to version 7.0.11
- Updated `aws-stream-consumer-core` dependency to version 2.0.4
- Updated `core-functions` dependency to version 3.0.20
- Updated `logging-utils` dependency to version 4.0.20
- Updated `task-utils` dependency to version 7.0.3
- Updated `aws-sdk` dev dependency to version 2.161.0

### 2.0.0
- More updates to `README.md`

### 2.0.0-beta.3
- Minor changes to `kinesis-consumer` module:
  - Renamed `generateSettings` argument to `createSettings`
  - Renamed `generateOptions` argument to `createOptions`
- Renamed dummy first exports (`exports._ = '_'; //IDE workaround`) of most modules to (`exports._$_ = '_$_';`) to avoid 
  potential future collisions with `lodash` & `underscore`
- Updated `core-functions` dependency to version 3.0.19
- Updated `logging-utils` dependency to version 4.0.19
- Updated `task-utils` dependency to version 7.0.2
- Updated `aws-core-utils` dependency to version 7.0.10 (to facilitate use of AWS XRay)
- Updated `aws-stream-consumer-core` dependency to version 2.0.3
- Updated `aws-sdk` dev dependency to version 2.143.0
- Updated `aws-core-test-utils` dev dependency to version 3.0.6

### 2.0.0-beta.2
- Upgraded `aws-stream-consumer-core` to version 2.0.1 - to patch `persisting` module issues

### 2.0.0-beta.1
- Refactored and extracted Kinesis stream consumer-specific logic from `aws-stream-consumer` into this new module
