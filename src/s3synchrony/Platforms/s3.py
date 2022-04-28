"""Contains the S3Connection class.

S3Connection - Data platform class for synchronizing with AWS S3.

Copyright (C) 2022  Sevan Brodjian
Created for Ameren at the Ameren Innovation Center @ UIUC

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""


import s3synchrony as s3s
from s3synchrony import BasePlatform

import py_starter as ps
import aws_connections
import aws_connections.s3 as s3


class Platform( BasePlatform ):

    util_dir = '.S3'

    DEFAULT_KWARGS = {
    'aws_bkt': None,
    'credentials': {}
    }

    DIR_CLASS = s3.S3Dir
    DIRS_CLASS = s3.S3Dirs
    PATH_CLASS = s3.S3Path
    PATHS_CLASS = s3.S3Paths

    def __init__(self, **kwargs ):

        joined_kwargs = ps.merge_dicts( Platform.DEFAULT_KWARGS, kwargs )
        BasePlatform.__init__( self, **joined_kwargs )

        if not s3.S3Dir.is_Dir( self.data_rDir ):
            self.data_rDir = s3.S3Dir( bucket = self.aws_bkt, path = self.remote_data_dir, conn = self.conn )

        self._util_rDir = self.data_rDir.join_Dir( path = self.util_dir ) #S3Dir
        self._util_deleted_rDir = self._util_rDir.join_Dir( path = 'deleted' ) #S3Dir
        self._remote_versions_rPath = self._util_rDir.join_Path( path = self._remote_versions_lPath.filename )
        self._remote_delete_rPath = self._util_rDir.join_Path( path = self._remote_delete_lPath.filename )


    def _get_remote_connection( self ):

        self.conn = aws_connections.Connection( "s3", **self.credentials )

