################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.java_gateway import get_gateway
from pyflink.common import RowKind
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ['ChangelogMode']


@PublicEvolving()
class ChangelogMode(object):
    """
    The set of changes contained in a changelog.
    """

    def __init__(self, j_changelog_mode):
        self._j_changelog_mode = j_changelog_mode
        self._contained_kinds = None
        self._key_only_deletes = None

    @staticmethod
    def _from_python(contained_kinds, key_only_deletes=False):
        mode = ChangelogMode(None)
        mode._contained_kinds = frozenset(contained_kinds)
        mode._key_only_deletes = key_only_deletes
        return mode

    @staticmethod
    def insert_only():
        """
        Shortcut for a simple :attr:`~pyflink.common.RowKind.INSERT`-only changelog.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.insertOnly())

    @staticmethod
    def upsert():
        """
        Shortcut for an upsert changelog that describes idempotent updates on a key and thus does
        does not contain :attr:`~pyflink.common.RowKind.UPDATE_BEFORE` rows.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.upsert())

    @staticmethod
    def all():
        """
        Shortcut for a changelog that can contain all :class:`~pyflink.common.RowKind`.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.all())

    def get_contained_kinds(self):
        """Returns the row kinds contained in this changelog mode."""
        if self._contained_kinds is None:
            self._contained_kinds = frozenset(
                RowKind[kind.name()] for kind in self._j_changelog_mode.getContainedKinds())
        return frozenset(self._contained_kinds)

    def contains(self, row_kind: RowKind) -> bool:
        """Returns whether this changelog mode contains the given row kind."""
        if not isinstance(row_kind, RowKind):
            raise TypeError("row_kind must be a pyflink.common.RowKind.")
        return row_kind in self.get_contained_kinds()

    def contains_only(self, row_kind: RowKind) -> bool:
        """Returns whether this changelog contains only the given row kind."""
        return self.contains(row_kind) and len(self.get_contained_kinds()) == 1

    def key_only_deletes(self) -> bool:
        """Returns whether delete rows contain only key columns."""
        if self._key_only_deletes is None:
            self._key_only_deletes = self._j_changelog_mode.keyOnlyDeletes()
        return self._key_only_deletes
