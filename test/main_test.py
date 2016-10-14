from __future__ import print_function
from __future__ import unicode_literals

import os

from atexit import register
from unittest import TestCase, main
from tempfile import mkdtemp
from time import sleep

try:
    import json as json
except ImportError:
    import json

from git import Repo
from shutil import copy, copytree, rmtree
from git_larder import GitRecord, GitRecordFactory, ModelIgnored, NoResultFound


temp_path = None


def maybe_make_test_repo():
    global temp_path
    temp_path = temp_path or mkdtemp()
    fixtures_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_repo')
    test_repo_path = os.path.join(temp_path, 'test_repo')
    test_repo = Repo.init(test_repo_path)

    # Copy files first, but not directories, to get an initial commit
    # This is because we want a commit without the model directory, to simulate that case
    for filename in os.listdir(fixtures_path):
        fullname = os.path.join(fixtures_path, filename)
        dstname = os.path.join(test_repo_path, filename)
        if os.path.isfile(fullname):
            copy(fullname, dstname)
    test_repo.index.add(['.'])
    test_repo.index.commit("Initialize test database")

    # Copy in the rest now
    for filename in os.listdir(fixtures_path):
        fullname = os.path.join(fixtures_path, filename)
        dstname = os.path.join(test_repo_path, filename)
        if os.path.isdir(fullname):
            copytree(fullname, dstname)

    test_repo.index.add(['ignored_model', 'ignored_model/*', '.gitrecord_ignore'])
    test_repo.index.commit("Second commit, adds actual files")
    test_repo.index.add(['test_model', 'test_model/*'])
    test_repo.index.commit("Third commit, adds model")

    sleep(1)  # To ensure commit time changes
    blob = test_repo.head.commit.tree['test_model/test_record_one.json']
    json_blob = json.load(blob.data_stream)
    json_blob['a_changed_attribute'] = 'some_changed_value'
    json.dump(json_blob, file(os.path.join(test_repo_path, 'test_model', 'test_record_one.json'), 'w'))
    test_repo.index.add(['test_model/test_record_one.json'])
    test_repo.index.commit('Commit after create')

    return test_repo, test_repo_path


def maybe_unmake_test_repo():
    global temp_path
    if temp_path:
        rmtree(temp_path)
        temp_path = None


register(maybe_unmake_test_repo)


class GitLarderTest(TestCase):
    def setUp(self):
        self._test_repo, self._test_repo_path = maybe_make_test_repo()
        self._test_repo_commit = self._test_repo.commit('HEAD')
        self._test_datastore = GitRecordFactory(self._test_repo)
        self._test_model = self._test_datastore.get_model("test_model")
        self._test_model.attach_to_datastore(self._test_datastore)

    def tearDown(self):
        self._test_repo.heads('master').reset(self._test_repo_commit, working_tree=True)

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
            self._test_datastore.find(ModelIgnored, 'ignored_file')

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
            self._test_model.find('test_record_one'),
            version=test_record_one['version'],
        )


def GitLarderVersionComparisonTests(GitLarderTest):
    def setUp(self):
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

        if not self.__class__._object_cache:
            self.__class__._object_cache, self.__class__._id_to_ref_map = self._test_model.build_object_cache()

        self._object_cache, self._id_to_ref_map = (
            self.__class__._object_cache,
            self.__class__._id_to_ref_map,
        )

    def in_memory_cache_has_expected_number_of_members_test(self):
        self.assertEqual(3, len(self._object_cache.keys()))
        self.assertEqual(2, len(self._id_to_ref_map.keys()))

    def in_memory_ref_map_is_identical_to_non_cached_test(self):
        self.assertEqual(
            self._test_model.find('test_record_one')['version'],
            self._id_to_ref_map['test_record_one'],
        )

        self.assertEqual(
            self._test_model.find('test_record_two')['version'],
            self._id_to_ref_map['test_record_two'],
        )

    def in_memory_object_cache_by_version_retrieves_correct_records(self):
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
            self._object_cache[test_record_one_earliest['version']],
        )

        self.assertEqual(
            test_record_one_latest,
            self._object_cache[test_record_one_latest['version']],
        )


if __name__ == '__main__':
    main()
