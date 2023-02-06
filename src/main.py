from ee_oss_commit_matcher import find_ee_commit, find_os_commit

os_commit = "f90a1ff43df11b8e08f8f2ac03c009b255692ce6"
ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"

result = find_ee_commit("/Users/serkan/forked/hazelcast", "/Users/serkan/forked/hazelcast-enterprise", os_commit)
assert result == ee_commit

result = find_os_commit("/Users/serkan/forked/hazelcast", "/Users/serkan/forked/hazelcast-enterprise", ee_commit)
assert result == os_commit
