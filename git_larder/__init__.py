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

from hashlib import sha1

try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

try:
    import simplejson as json
except ImportError:
    import json

from git import Repo, Git
from git.objects.blob import Blob
from gitdb.exc import BadObject, BadName


# Chunking method, courtesy J.F.Sebastian from
# http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
# (itself modified from an itertools documentation recipe)
def chunk_into_groups_of(n, iterable, padvalue=None):
    """Given an iterable, return an iterator that groups it into n-sized chunks"""
    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)


class NoResultFound(Exception):
    def __init__(self, *args, **kwargs):
        last_version = kwargs.pop('last_version', None)
        super(NoResultFound, self).__init__(*args, **kwargs)
        self.last_version = last_version


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


def _blob_to_cache_key(blob):
    return version_to_cache_key(_id_from_blob(blob), blob.hexsha)


def version_to_cache_key(plan_id, version):
    try:
        plan_id = plan_id.encode('utf8')
    except AttributeError:
        pass

    try:
        version = version.encode('utf8')
    except AttributeError:
        pass

    h = sha1()
    h.update(plan_id + version)

    return h.hexdigest()


class GitRecordFactory(object):
    def __init__(self, repo_location, no_result_found_error_class=None):
        self._repo = Repo(repo_location)
        self._git = Git(repo_location)
        self._no_result_found = no_result_found_error_class or NoResultFound
        try:
            for line in self._repo.head.commit.tree['.gitrecord_ignore'].data_stream.read().decode('utf8').split('\n'):
                self._ignored = line.strip()
        except (ValueError, KeyError):
            self._ignored = []

    def all(self, record_model):
        self._verify_model_exists(record_model)
        commit = self._repo.head.commit

        tree = commit.tree[record_model.__modelname__]

        models = []
        for b in tree.blobs:
            try:
                loaded_record = _load_record_from_blob(b, commit)
                models.append(record_model(loaded_record))
            except ValueError:
                pass

        return models

    def get_version(self):
        return self._repo.head.commit.hexsha

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
                cache_key = _blob_to_cache_key(b)
                if last_commit:
                    # Populate a map so we can grab head objects by id rather than version
                    id_to_ref_map[_id_from_blob(b)] = cache_key

                try:
                    cache[cache_key] = cache.get(cache_key, _load_record_from_blob(b, commit))
                except ValueError as e:
                    if last_commit:
                        # If this happens, we have a bad record in HEAD, and we need to bail
                        raise ValueError('Bad record %s found at %s: %s' % (_id_from_blob(b), commit, e))

            last_commit = False

        return cache, id_to_ref_map

    def _get_all_commits_for_path(self, path):
        hexshas = self._git.log('--pretty=%H', '--follow', '--', path).split('\n')
        return [self._repo.rev_parse(c) for c in hexshas]

    def _get_all_commits_for_path_with_paths(self, path):
        hexshas_with_paths = []
        raw_output = self._git.log('--pretty=%H', '--follow', '--name-status', '--', path)
        # pretty=%H --name-status produces output that is three lines per commit:
        #   <commit hash>
        #   <blank line>
        #   <commit status, (A)dded, (M)odified, (C)hanged, (D)eleted, (R)enamed>\t<original path>[\t<final path if C or R>]

        if not raw_output:
            return []

        raw_output_lines = raw_output.split('\n')
        for (commit_hash, _, path) in chunk_into_groups_of(3, raw_output_lines):
            status, path_or_paths = path.split('\t', 1)
            status = status[0:1]
            if status in ['A', 'M']:
                hexshas_with_paths.append((commit_hash, path_or_paths))
            elif status in ['C', 'R']:
                changed_from, changed_to = path_or_paths.split('\t')
                hexshas_with_paths.append((commit_hash, changed_to))

        return [(self._repo.rev_parse(c), p) for c, p in hexshas_with_paths]

    def _get_last_commit_for_deleted_path(self, path):
        raw_output = self._git.log('--pretty=%H', '--name-only', '--diff-filter=D')

        if not raw_output:
            return None

        last_line = None
        current_commit_ref = None

        for line in raw_output.split('\n'):
            # output looks like <commit\n\n<file_deleted_that_commit>\n...>...
            if not line:
                current_commit_ref = last_line
            elif line == path:
                return self._repo.rev_parse(current_commit_ref).parents[0]

            last_line = line

    def _verify_model_exists(self, record_model):
        if record_model.__modelname__ in self._ignored:
            raise ModelIgnored("That model exists, but is ignored by .gitrecord_ignore")

        try:
            self._repo.head.commit.tree[record_model.__modelname__]
        except (KeyError, ValueError):
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
                #  We can't use this directly because it won't get us a last_updated without the commit
                test_blob = Blob(self._repo, binascii.a2b_hex(version))
                test_blob.data_stream  # Throws an error if the blob isn't in the repo

            except (BadObject, BadName):
                raise self._no_result_found("Version did not exist: %s" % version)

            # Search in the list of all versions for this particular sha
            # TODO: eventually make this an actual search algorithm, because it will probably get really slow with lots of commits
            commits_with_paths = self._get_all_commits_for_path_with_paths(path)

            for commit, commit_path in commits_with_paths:
                blob = commit.tree[commit_path]
                if blob.hexsha == version:
                    try:
                        loaded_record = _load_record_from_blob(blob, commit)
                        return record_model(loaded_record)
                    except ValueError as e:
                        raise self._no_result_found("Version specified was invalid JSON: %s" % e)

        elif all_versions:
            commits_with_paths = self._get_all_commits_for_path_with_paths(path)
            records = []
            if retrieve_max:
                commits_with_paths = commits_with_paths[:retrieve_max]

            for commit, commit_path in commits_with_paths:
                blob = commit.tree[commit_path]
                try:
                    loaded_record = _load_record_from_blob(blob, commit)
                    records.append(record_model(loaded_record))
                except (KeyError, ValueError):
                    pass

            return records

        else:
            commit = self._repo.head.commit

        try:
            blob = commit.tree[path]
            loaded_record = _load_record_from_blob(blob, commit)
            record = record_model(loaded_record)
        except (KeyError, ValueError):
            last_commit = self._get_last_commit_for_deleted_path(path)
            if last_commit:
                blob = last_commit.tree[path]
                try:
                    loaded_record = _load_record_from_blob(blob, last_commit)
                    raise self._no_result_found("No record found with that id (but a previous version was found: %s)" % loaded_record['version'], last_version=loaded_record)
                except ValueError as e:
                    raise self._no_result_found("No record found with that id. A previous version was found (%s), but was invalid JSON (%s" % (last_commit, e))
            else:
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
    def get_version(cls, *args, **kwargs):
        return cls.get_factory().get_version(*args, **kwargs)

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
    version_to_cache_key,
]
