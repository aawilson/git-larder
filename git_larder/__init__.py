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

import binascii
import os

try:
    import simplejson as json
except ImportError:
    import json

from git import Repo, Git
from git.objects.blob import Blob
from gitdb.exc import BadObject


class NoResultFound(KeyError):
    pass


class ModelIgnored(KeyError):
    pass


def _id_from_blob(blob):
    return os.path.splitext(blob.name)[0]


def _load_record_from_blob(blob, commit):
    loaded_record = json.loads(blob.data_stream.read().decode('utf8'))
    loaded_record.update({
        'id': _id_from_blob(blob),
        'version': blob.hexsha,
        'updated_at': commit.committed_date,
    })

    return loaded_record


class GitRecordFactory(object):
    def __init__(self, repo_location, no_result_found_error_class=None):
        self._repo = Repo(repo_location)
        self._git = Git(repo_location)
        self._no_result_found = no_result_found_error_class or NoResultFound
        try:
            for line in self._repo.head.commit.tree['.gitrecord_ignore'].data_stream.read().decode('utf8').split('\n'):
                self._ignored = line.strip()
        except KeyError:
            self._ignored = []

    def all(self, record_model):
        self._verify_model_exists(record_model)
        commit = self._repo.head.commit

        tree = commit.tree[record_model.__modelname__]

        models = []
        for b in tree.blobs:
            loaded_record = _load_record_from_blob(b, commit)
            models.append(record_model(loaded_record))

        return models

    def build_object_cache(self, record_model):
        cache = {}
        id_to_ref_map = {}

        self._verify_model_exists(record_model)
        last_commit = True
        for commit in self._repo.iter_commits(self._repo.head):
            try:
                tree = commit.tree[record_model.__modelname__]
            except KeyError:
                # This path may not have existed for all commits, we're fine with that.
                continue

            for b in tree.blobs:
                if last_commit:
                    # Populate a map so we can grab head objects by id rather than version
                    id_to_ref_map[_id_from_blob(b)] = b.hexsha

                cache[b.hexsha] = cache.get(b.hexsha, _load_record_from_blob(b, commit))

            last_commit = False

        return cache, id_to_ref_map

    def _get_all_commits_for_path(self, path):
        hexshas = self._git.log('--pretty=%H', '--follow', '--', path).split('\n')
        return [self._repo.rev_parse(c) for c in hexshas]

    def _verify_model_exists(self, record_model):
        if record_model.__modelname__ in self._ignored:
            raise ModelIgnored("That model exists, but is ignored by .gitrecord_ignore")

        try:

            self._repo.head.commit.tree[record_model.__modelname__]
        except KeyError:
            raise self._no_result_found("That model did not exist in the database")

        return True

    def _path_for_name(self, record_model, name):
        return "%s/%s.json" % (record_model.__modelname__, name)

    def find(self, record_model, name=None, version=None, all_versions=False, retrieve_max=None):
        record = None
        commit = None
        path = self._path_for_name(record_model, name)

        self._verify_model_exists(record_model)

        if version and all_versions:
            raise ValueError('Cannot simultaneously search for a particular version and all versions of a record')

        if version:
            try:
                # load directly from the object database, to save us a search if it doesn't exist
                test_blob = Blob(self._repo, binascii.a2b_hex(version))
                test_blob.data_stream  # Throws an error if the blob isn't in the repo

            except BadObject:
                raise self._no_result_found("No record found of that version")

            # Search in the list of all versions for this particular sha
            # TODO: eventually make this an actual search algorithm, because it will probably get really slow with lots of commits
            commits = self._get_all_commits_for_path(path)

            for commit in commits:
                blob = commit.tree[path]
                if blob.hexsha == version:
                    loaded_record = _load_record_from_blob(blob, commit)
                    return record_model(loaded_record)

        elif all_versions:
            commits = self._get_all_commits_for_path(path)
            records = []
            if retrieve_max:
                commits = commits[:retrieve_max]

            for commit in commits:
                blob = commit.tree[path]
                loaded_record = _load_record_from_blob(blob, commit)
                records.append(record_model(loaded_record))

            return records

        else:
            commit = self._repo.head.commit

        try:
            blob = commit.tree[path]
            loaded_record = _load_record_from_blob(blob, commit)
            record = record_model(loaded_record)
        except KeyError:
            raise self._no_result_found("No record found with that id")

        return record

    def reset(self, record=None):
        if record:
            path = self._path_for_name(record.__class__, record['id'])
            self._repo.index.checkout(path, force=True)
        else:
            self._repo.head.reset(None, index=True, working_tree=True)
            # self._repo.head.reset([diff.a_blob.name for diff in self._repo.index.diff(None)])

    def get_model(self, model_name):
        class NewClass(GitRecord):
            __name__ = "GitRecord_%s" % model_name
            __modelname__ = model_name

        self._verify_model_exists(NewClass)

        NewClass.attach_to_datastore(self)
        return NewClass


class GitRecord(dict):
    """
    The class used to query (and whose instances contain) information from
    the repo.

    Subclasses should define a "__modelname__" attribute.
    It corresponds to the subpath within the repo.
    """
    @classmethod
    def get_factory(cls):
        if getattr(cls, '_factory', None):
            return cls._factory
        else:
            raise AttributeError('No datastore attached to model %s' % cls.__name__)

    @classmethod
    def attach_to_datastore(cls, factory):
        cls._factory = factory

    @classmethod
    def detach_from_datastore(cls):
        cls._factory = None

    def reload(self):
        self._factory.reset(self)
        name = self['id']

        for k in list(self.keys()):
            del self[k]

        self.update(self._factory.find(self.__class__, name=name))

    @classmethod
    def find(cls, *args, **kwargs):
        return cls.get_factory().find(cls, *args, **kwargs)

    @classmethod
    def build_object_cache(cls, *args, **kwargs):
        return cls.get_factory().build_object_cache(cls, *args, **kwargs)

    @classmethod
    def all(cls, *args, **kwargs):
        return cls.get_factory().all(cls, *args, **kwargs)

    @classmethod
    def _path_for_name(cls, *args, **kwargs):
        return cls.get_factory()._path_for_name(cls, *args, **kwargs)


__all__ = [
    GitRecord,
    GitRecordFactory,
    ModelIgnored,
    NoResultFound,
]
