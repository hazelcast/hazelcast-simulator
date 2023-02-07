import unittest
import os
from ee_oss_commit_matcher import find_ee_commit, find_os_commit

class TestFindEECommit(unittest.TestCase):
    def setUp(self):
        self.oss_repo_path = os.environ.get("OSS_REPO_PATH")
        self.ee_repo_path = os.environ.get("EE_REPO_PATH")

        if self.oss_repo_path is None or self.ee_repo_path is None:
            raise Exception("OSS_REPO_PATH and EE_REPO_PATH environment variables need to be set for this test")

    def test_raises_with_invalid_oss_repo(self):
        # The commit is not important in this test.
        oss_commit = "1ab727191c858afcfc46cc1641fdf63c5ad761c5"
        with self.assertRaises(Exception):
            find_ee_commit("/doesnt/exist", self.ee_repo_path, oss_commit)

        # The validity of OSS repo is checked by finding a remote with url that has hazelcast/hazelcast in it.
        with self.assertRaises(Exception):
            find_ee_commit(self.ee_repo_path, self.ee_repo_path, oss_commit)

    def test_raises_with_invalid_ee_repo(self):
        # The commit is not important in this test.
        oss_commit = "1ab727191c858afcfc46cc1641fdf63c5ad761c5"
        with self.assertRaises(Exception):
            find_ee_commit(self.oss_repo_path, "/doesnt/exist", oss_commit)
        
        # The validity of EE repo is checked by finding a remote with url that has hazelcast/hazelcast-enterprise in it.
        with self.assertRaises(Exception):
            find_ee_commit(self.oss_repo_path, self.oss_repo_path, oss_commit)

    def test_master_commit(self):
        # This OSS commit is a master commit
        oss_commit = "f90a1ff43df11b8e08f8f2ac03c009b255692ce6"
        # This is the corresponding EE commit. This is the earliest commit on master that is after the OSS commit.
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        self.assertEqual(find_ee_commit(self.oss_repo_path, self.ee_repo_path, oss_commit), ee_commit)

    def test_maintenance_commit(self):
        # This OSS commit is a commit in 5.0.z maintenance branch
        oss_commit = "a2815f4befd184a8cf0b86978520793b80db639c"
        # This is the corresponding EE commit. This is the earliest commit on 5.0.z that is after the OSS commit.
        ee_commit = "9e80ad303ce84780b09908204c3e1b2ec6e62a19"
        self.assertEqual(find_ee_commit(self.oss_repo_path, self.ee_repo_path, oss_commit), ee_commit)

    def test_exact_version(self):
        # This OSS commit is the commit of v5.2.0 tag.
        oss_commit = "1ab727191c858afcfc46cc1641fdf63c5ad761c5"
        # This is the corresponding EE commit. This is the v5.2.0 tag commit.
        ee_commit = "b467705392805fe520b40c9f30fa01606797763b"
        self.assertEqual(find_ee_commit(self.oss_repo_path, self.ee_repo_path, oss_commit), ee_commit)

class TestFindOSCommit(unittest.TestCase):
    def setUp(self):
        self.oss_repo_path = os.environ.get("OSS_REPO_PATH")
        self.ee_repo_path = os.environ.get("EE_REPO_PATH")

        if self.oss_repo_path is None or self.ee_repo_path is None:
            raise Exception("OSS_REPO_PATH and EE_REPO_PATH environment variables need to be set for this test")

    def test_raises_with_invalid_oss_repo(self):
        # The commit is not important in this test.
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        with self.assertRaises(Exception):
            find_os_commit("/doesnt/exist", self.ee_repo_path, ee_commit)

        # The validity of OSS repo is checked by finding a remote with url that has hazelcast/hazelcast in it.
        with self.assertRaises(Exception):
            find_os_commit(self.ee_repo_path, self.ee_repo_path, ee_commit)

    def test_raises_with_invalid_ee_repo(self):
        # The commit is not important in this test.
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        with self.assertRaises(Exception):
            find_os_commit(self.oss_repo_path, "/doesnt/exist", ee_commit)
        
        # The validity of EE repo is checked by finding a remote with url that has hazelcast/hazelcast-enterprise in it.
        with self.assertRaises(Exception):
            find_os_commit(self.oss_repo_path, self.oss_repo_path, ee_commit)

    def test_master_commit(self):
        # This EE commit is a master commit
        ee_commit = "b1ffcbdec8cd0c7699a2d2aaa8b4137107f41f3d"
        # This is the corresponding OSS commit. This is the latest commit on master before the EE commit.
        oss_commit = "f55912b0819c576cb6cd5ed2e5f57e402ac207a4"
        self.assertEqual(find_os_commit(self.oss_repo_path, self.ee_repo_path, ee_commit), oss_commit)

    def test_maintenance_commit(self):
        # This EE commit is a commit in 5.2.z maintenance branch
        ee_commit = "31790d00f0b02c9220fe399ea835a345be9a92ac"
        # This is the corresponding OSS commit. This is the latest commit on 5.2.z before the EE commit.
        oss_commit = "4882875223c2caa6f78532df44ddd22e7e061ca5"
        self.assertEqual(find_os_commit(self.oss_repo_path, self.ee_repo_path, ee_commit), oss_commit)

    def test_exact_version(self):
        # This EE commit is the commit of v5.2.0 tag.
        ee_commit = "b467705392805fe520b40c9f30fa01606797763b"
        # This is the corresponding OSS commit. This is the v5.2.0 tag commit.
        oss_commit = "1ab727191c858afcfc46cc1641fdf63c5ad761c5"
        self.assertEqual(find_os_commit(self.oss_repo_path, self.ee_repo_path, ee_commit), oss_commit)


"""
Running this test file can be done like this:

SIMULATOR_HOME=/path/to/hazelcast-simulator OSS_REPO_PATH=/path/to/hazelcast/repo EE_REPO_PATH=/path/to/hazelcast/enterprise/repo python3 ee_oss_commit_matcher_test.py
"""
if __name__ == '__main__':
    unittest.main()
