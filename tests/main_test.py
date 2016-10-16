"""
Copyright 2016 Aaron Wilson and Habla, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from __future__ import print_function
from __future__ import unicode_literals

import os
import tempfile
import time
import unittest
import shutil

try:
    import json as json
except ImportError:
    import json

from git import Repo
from git_larder import GitRecord, GitRecordFactory, ModelIgnored, NoResultFound, version_to_cache_key


class GlobalTestState(object):
    temp_path = None
    test_repo = None
    test_repo_path = None

    object_cache = None
    id_to_ref_map = None


gts = GlobalTestState()


def get_raw_json_from_record(repo, record_id):
    blob = repo.head.commit.tree['test_model/%s.json' % record_id]
    return json.loads(blob.data_stream.read().decode('utf8'))


def record_to_cache_key(record):
    return version_to_cache_key(record['id'], record['version'])


def maybe_make_test_repo():
    if gts.temp_path:
        return gts.test_repo, gts.test_repo_path

    temp_path = gts.temp_path or tempfile.mkdtemp()
    fixtures_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_repo')
    test_repo_path = os.path.join(temp_path, 'test_repo')
    test_repo = Repo.init(test_repo_path)

    # Copy files first, but not directories, to get an initial commit
    # This is because we want a commit without the model directory, to simulate that case
    for filename in os.listdir(fixtures_path):
        fullname = os.path.join(fixtures_path, filename)
        dstname = os.path.join(test_repo_path, filename)
        if os.path.isfile(fullname):
            shutil.copy(fullname, dstname)
    test_repo.index.add(['.'])
    test_repo.index.commit("Initialize test database")

    # Copy in the rest now
    for filename in os.listdir(fixtures_path):
        fullname = os.path.join(fixtures_path, filename)
        dstname = os.path.join(test_repo_path, filename)
        if os.path.isdir(fullname):
            shutil.copytree(fullname, dstname)

    test_repo.index.add(['ignored_model', 'ignored_model/*', '.gitrecord_ignore'])
    test_repo.index.commit("Second commit, adds actual files")
    test_repo.index.add(['test_model', 'test_model/*'])
    test_repo.index.commit("Third commit, adds model")

    time.sleep(1)  # To ensure commit time changes
    blob = test_repo.head.commit.tree['test_model/test_record_one.json']
    json_blob = json.loads(blob.data_stream.read().decode('utf8'))
    json_blob['a_changed_attribute'] = 'some_changed_value'
    with open(os.path.join(test_repo_path, 'test_model', 'test_record_one.json'), 'w') as f:
        json.dump(json_blob, f)
    test_repo.index.add(['test_model/test_record_one.json'])
    test_repo.index.commit("Commit after create")

    test_repo.index.remove(['test_model/deleteme.json'])
    test_repo.index.commit("Commit after test remove")

    os.rename(
        os.path.join(test_repo_path, 'test_model/moveme.json'),
        os.path.join(test_repo_path, 'test_model/movedme.json'),
    )
    test_repo.index.remove(['test_model/moveme.json'])
    test_repo.index.add(['test_model/movedme.json'])

    test_repo.index.commit("Commit after test move")

    gts.temp_path, gts.test_repo, gts.test_repo_path = temp_path, test_repo, test_repo_path

    return test_repo, test_repo_path


def maybe_unmake_test_repo():
    if not gts.temp_path:
        return

    shutil.rmtree(gts.temp_path, ignore_errors=True)


def setUpModule():
    maybe_make_test_repo()


def tearDownModule():
    maybe_unmake_test_repo()


class GitLarderTest(unittest.TestCase):
    def setUp(self):
        self._test_repo, self._test_repo_path = maybe_make_test_repo()
        self._test_repo_commit = self._test_repo.commit('HEAD')
        self._test_datastore = GitRecordFactory(self._test_repo_path)
        self._test_model = self._test_datastore.get_model("test_model")
        self._test_model.attach_to_datastore(self._test_datastore)

    def tearDown(self):
        self._test_repo.head.reset(self._test_repo_commit, working_tree=True)

    def timestamps_exist_test(self):
        for r in self._test_datastore.all(self._test_model):
            try:
                self.assertIsNotNone(r['updated_at'])
            except KeyError:
                self.fail('timestamp did not exist on retrieval from model')

    def find_by_name_from_factory_test(self):
        self.assertIsNotNone(self._test_datastore.find(self._test_model, 'test_record_one'))

    def find_by_name_from_model_test(self):
        self.assertIsNotNone(self._test_model.find('test_record_one'))

    def find_by_nonexistent_model_test(self):
        class NotAModel(GitRecord):
            __modelname__ = 'not_a_model'

        with self.assertRaises(NoResultFound):
            self._test_datastore.find(NotAModel, 'test_record_one')

    def find_by_ignored_model_test(self):
        class IgnoredModel(GitRecord):
            __modelname__ = 'ignored_model'

        with self.assertRaises(ModelIgnored):
            self._test_datastore.find(IgnoredModel, 'ignored_file')

    def find_by_nonexistent_id_test(self):
        with self.assertRaises(NoResultFound):
            self._test_model.find('not_a_record')

    def find_without_datastore_fails_test(self):
        self._test_model.detach_from_datastore()

        with self.assertRaises(AttributeError):
            self._test_model.find('test_record_one')

    def get_model_verifys_model_exists_test(self):
        with self.assertRaises(NoResultFound):
            self._test_datastore.get_model('not_a_model')

    def model_modify_test(self):
        test_record_one = self._test_model.find('test_record_one')
        self.assertIn('test_integer_attribute', test_record_one)

        old_val = test_record_one['test_integer_attribute']

        test_record_one['test_integer_attribute'] += 1
        self.assertEqual(old_val + 1, test_record_one['test_integer_attribute'])

    def model_reload_test(self):
        test_record_one = self._test_model.find('test_record_one')

        old_val = test_record_one['test_integer_attribute']
        test_record_one['test_integer_attribute'] += 1

        test_record_one.reload()
        self.assertEqual(old_val, test_record_one['test_integer_attribute'])

    def model_assert_versions_test(self):
        self.assertEqual(
            2,
            len(self._test_model.find('test_record_one', all_versions=True)),
        )

    def model_all_versions_retrieval_has_timestamp_test(self):
        for r in self._test_model.find('test_record_one', all_versions=True):
            try:
                self.assertIsNotNone(r['updated_at'])
            except KeyError:
                self.fail(
                    "timestamp did not exist when attempting retrieval with"
                    " all_versions flag",
                )

    def model_find_by_version_test(self):
        test_record_one = self._test_model.find('test_record_one')

        self.assertIsNotNone(
            self._test_model.find(
                'test_record_one',
                version=test_record_one['version'],
            )
        )

    def model_find_deleted_test(self):
        with self.assertRaises(NoResultFound) as cm:
            self._test_model.find('deleteme')

        self.assertIsNotNone(cm.exception.last_version)
        self.assertEqual('deleteme', cm.exception.last_version['id'])

    def model_find_moved_test(self):
        with self.assertRaises(NoResultFound):
            self._test_model.find('moveme')

        self.assertGreater(len(self._test_model.find('movedme', all_versions=True)), 1)


class GitLarderVersionComparisonTest(GitLarderTest):
    def setUp(self):
        super(GitLarderVersionComparisonTest, self).setUp()

        self._test_record_one_all_versions = self._test_model.find(
            'test_record_one',
            all_versions=True)

        self._test_record_one_latest = self._test_model.find(
            'test_record_one',
            version=self._test_record_one_all_versions[0]['version'],
        )
        self._test_record_one_earliest = self._test_model.find(
            'test_record_one',
            version=self._test_record_one_all_versions[-1]['version'],
        )

    def model_versions_distinct_test(self):
        self.assertNotEqual(
            self._test_record_one_latest['version'],
            self._test_record_one_earliest['version'],
        )

    def model_version_order_test(self):
        self.assertTrue(
            self._test_record_one_latest['updated_at'] > self._test_record_one_earliest['updated_at']
        )

    def model_version_attributes_distinct_test(self):
        self.assertNotIn('a_changed_attribute', self._test_record_one_earliest)
        self.assertIn('a_changed_attribute', self._test_record_one_latest)


class InMemoryCacheTest(GitLarderTest):
    def setUp(self):
        super(InMemoryCacheTest, self).setUp()

        if not gts.object_cache:
            gts.object_cache, gts.id_to_ref_map = self._test_model.build_object_cache()

        self.object_cache, self.id_to_ref_map = (
            gts.object_cache,
            gts.id_to_ref_map,
        )

    def in_memory_cache_has_expected_number_of_members_test(self):
        self.assertEqual(8, len(self.object_cache.keys()))
        self.assertEqual(5, len(self.id_to_ref_map.keys()))

    def in_memory_ref_map_is_identical_to_non_cached_test(self):
        self.assertEqual(
            version_to_cache_key('test_record_one', self._test_model.find('test_record_one')['version']),
            self.id_to_ref_map['test_record_one'],
        )

        self.assertEqual(
            version_to_cache_key('test_record_two', self._test_model.find('test_record_two')['version']),
            self.id_to_ref_map['test_record_two'],
        )

    def version_to_cache_key_is_fine_with_bytes_test(self):
        self.assertIsNotNone(version_to_cache_key(b'fake_record', b'fake_version'))

    def in_memory_object_cache_by_version_retrieves_correct_records_test(self):
        test_record_one_all_versions = self._test_model.find(
            'test_record_one',
            all_versions=True)

        test_record_one_latest = self._test_model.find(
            'test_record_one',
            version=test_record_one_all_versions[0]['version'],
        )
        test_record_one_earliest = self._test_model.find(
            'test_record_one',
            version=test_record_one_all_versions[-1]['version'],
        )

        self.assertEqual(
            test_record_one_earliest,
            self.object_cache[record_to_cache_key(test_record_one_earliest)],
        )

        self.assertEqual(
            test_record_one_latest,
            self.object_cache[record_to_cache_key(test_record_one_latest)],
        )

    def cache_keys_are_not_identical_for_identical_content_test(self):
        r1_raw_json = get_raw_json_from_record(self._test_repo, 'identical1')
        r2_raw_json = get_raw_json_from_record(self._test_repo, 'identical2')

        self.assertEqual(r1_raw_json, r2_raw_json)

        r1 = self._test_model.find('identical1')
        r2 = self._test_model.find('identical2')

        ref_record_1 = record_to_cache_key(r1)
        ref_record_2 = record_to_cache_key(r2)

        self.assertNotEqual(ref_record_1, ref_record_2)


class InvalidJSONTest(GitLarderTest):
    def _write_raw_to_invalid_record(self, data):
        file_path = os.path.join(self._test_repo_path, 'test_model', 'invalid.json')

        with file(file_path, 'w') as f:
            f.write(data)

        self._test_repo.index.add(['test_model/invalid.json'])

    def setUp(self):
        super(InvalidJSONTest, self).setUp()

        self._write_raw_to_invalid_record('{"invalid": 1')
        self._test_repo.index.commit('Initialize bad record')

    def find_with_invalid_record_returns_no_result_test(self):
        with self.assertRaises(NoResultFound):
            self._test_model.find('invalid')

    def find_with_valid_record_returns_result_when_invalid_exists_test(self):
        try:
            self.assertIsNotNone(self._test_model.find('test_record_one'))
        except NoResultFound:
            self.fail("Valid record query should not raise exception")

    def find_with_valid_record_with_invalid_previous_version_test(self):
        self._write_raw_to_invalid_record('{"valid": 1}')
        self._test_repo.index.commit("Change invalid record to valid")

        try:
            self._test_model.find('invalid')
        except NoResultFound:
            self.fail("Valid record query with invalid version in history should not raise")

    def find_all_versions_with_valid_record_with_invalid_previous_version_test(self):
        self._write_raw_to_invalid_record('{"valid": 1}')
        self._test_repo.index.commit("Change invalid record to valid")

        try:
            self.assertEqual(1, len(self._test_model.find('invalid', all_versions=True)))
        except NoResultFound:
            self.fail("Valid record query with invalid version in history should not raise")

    def build_cache_with_invalid_version_at_head_fails_test(self):
        with self.assertRaises(ValueError):
            self._test_datastore.build_object_cache(self._test_model)

    def build_cache_with_invalid_version_in_history_succeeds_test(self):
        self._write_raw_to_invalid_record('{"valid": 1}')
        self._test_repo.index.commit("Change invalid record to valid")

        self._test_datastore.build_object_cache(self._test_model)

    def record_does_not_populate_last_version_after_delete_if_last_version_was_invalid_test(self):
        self._test_repo.index.remove(['test_model/invalid.json'])
        self._test_repo.index.commit("Delete invalid blob")

        try:
            self._test_model.find("invalid")
        except NoResultFound as e:
            self.assertIsNone(e.last_version)


if __name__ == '__main__':
    unittest.main()
