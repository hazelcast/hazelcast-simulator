To run tests, run the following command in the root folder:

```
OSS_REPO_PATH=/path/to/hazelcast/repo EE_REPO_PATH=/path/to/hazelcast/enterprise/repo python3 -m unittest
```
a
OSS_REPO_PATH and EE_REPO_PATH are paths to a hazelcast and hazelcast-enterprise repos respectively. They are needed for some tests, e.g ee-oss commit matcher test.