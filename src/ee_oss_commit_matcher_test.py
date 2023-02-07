import unittest
import os
from ee_oss_commit_matcher import find_ee_commit, find_os_commit

class TestFindEECommit(unittest.TestCase):
    def setUp(self):
        self.oss_repo_path = os.environ.get("OSS_REPO_PATH")
        self.ee_repo_path = os.environ.get("EE_REPO_PATH")

        if self.oss_repo_path is None or self.ee_repo_path is None:
            raise Exception("OSS_REPO_PATH and EE_REPO_PATH environment variables need to be set for this test")

    def test_master_commit(self):
        # This OSS commit is a master commit
        oss_commit = "f90a1ff43df11b8e08f8f2ac03c009b255692ce6"
        # This is the corresponding EE commit
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        self.assertEqual(find_ee_commit(self.oss_repo_path, self.ee_repo_path, oss_commit), ee_commit)

class TestFindOSSCommit(unittest.TestCase):
    def setUp(self):
        self.oss_repo_path = os.environ.get("OSS_REPO_PATH")
        self.ee_repo_path = os.environ.get("EE_REPO_PATH")

        if self.oss_repo_path is None or self.ee_repo_path is None:
            raise Exception("OSS_REPO_PATH and EE_REPO_PATH environment variables need to be set for this test")

    def test_master_commit(self):
        # This EE commit is a master commit
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        # This is the corresponding OSS commit
        oss_commit = "f55912b0819c576cb6cd5ed2e5f57e402ac207a4"
        self.assertEqual(find_os_commit(self.oss_repo_path, self.ee_repo_path, ee_commit), oss_commit)


"""
Running this test file can be done like this:

SIMULATOR_HOME=/path/to/hazelcast-simulator OSS_REPO_PATH=/path/to/hazelcast/repo EE_REPO_PATH=/path/to/hazelcast/enterprise/repo python3 ee_oss_commit_matcher_test.py
"""
if __name__ == '__main__':
    unittest.main()