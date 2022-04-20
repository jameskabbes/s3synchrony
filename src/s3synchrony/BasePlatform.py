"""Contains the DataPlatformConnection class.

DataPlatformConnection - Default data platform class from which others should inherit.

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

import hashlib
import datetime as dt

import s3synchrony as s3s
import py_starter as ps
import dir_ops as do
from parent_class import ParentClass

class BasePlatform( ParentClass ):

    """Default class for data platforms.

    This class should only be instantiated as a backup case when unrecognized
    input is provided. Otherwise, this class should be used as an interface to
    be inherited from by future connections. All public methods listed here
    should be overridden by the child classes.
    """

    DEFAULT_KWARGS = {
        'local_data_rel_dir': "Data",
        'remote_data_dir': "Data",
        'data_lDir': None,
        'data_rDir': None,
        '_name' : 'NONAME'
    }

    _file_colname = "File"
    _editor_colname = "Edited By"
    _time_colname = "Time Edited"
    _hash_colname = "Checksum"
    columns = [_file_colname, _editor_colname, _time_colname, _hash_colname]
    dttm_format = "%Y-%m-%d %H:%M:%S"


    def __init__(self, **kwargs):
        """Initialize necessary instance variables."""

        ParentClass.__init__( self )
        joined_kwargs = ps.merge_dicts( BasePlatform.DEFAULT_KWARGS, kwargs )
        self.set_atts( joined_kwargs )

        #
        if not do.Dir.is_Dir( self.data_lDir ):
            self.data_lDir = do.Dir( s3s._cwd_Dir.join( self.local_data_rel_dir ) )
        if not do.Dir.is_Dir( self.data_rDir ):
            self.data_rDir = do.Dir( self.remote_data_dir )

        #lDir is a local Dir, rDir is a remote dir
        self._util_lDir =  do.Dir( self.data_lDir.join(  self.util_dir ) )
        self._util_rDir = do.Dir( self.data_rDir.join( self.util_dir ) )

        self._remote_versions_lPath = do.Path( self._util_lDir.join( 'versions_remote.csv' ) )
        self._local_versions_lPath =  do.Path( self._util_lDir.join( 'versions_local.csv' ) )

        self._remote_delete_lPath = do.Path( self._util_lDir.join( 'deleted_remote.csv' ) )
        self._local_delete_lPath = do.Path( self._util_lDir.join( 'deleted_local.csv' ) )

        self._tmp_lDir = do.Dir( self._util_lDir.join('tmp') )
        self._logs_lDir = do.Dir( self._util_lDir.join('logs') )
        self._ignore_lPath = do.Path( self._util_lDir.join( 'ignore_remote.txt' ) )

        self._ignore = []
        self._logname = dt.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_")
        self._logname += self._get_randomized_dirname()[10:]
        self._logname = self._logspath + '/' + self._logname + ".txt"
        self._log = ""
        self._reset_approved = False

    def run( self ):

        self.intro_message()
        self.establish_connection()
        self.synchronize()
        self.close_message()

    def reset_all( self ):

        self.intro_message()
        self.establish_connection()
        if self.reset_confirm():
            self.reset_local()
            self.reset_remote()
        self.close_message()

    def intro_message(self):
        """Print an introductory message to signal program start."""
        print()
        print("###########################")
        print("#        Data Sync        #")
        print("###########################")
        print()

    def close_message(self):
        """Print a closing message to signal program end."""
        print()
        print("###########################")
        print("#       Done Syncing      #")
        print("###########################")

    def establish_connection(self):
        """Form a connection to the remote repository."""

        # Create data dir
        if not self.data_lDir.exists():
            self.data_lDir.create() 
            print ('Directory not present, creating empty directory at: ')
            print (self.data_lDir)

        # Create platform util local dir
        if not self._util_lDir.exists():
            self._util_lDir.create()

        # Create platform util remote dir
        subfolders = self._connect_to_remote()
        

    def _connect_to_remote( self ):

        pass

    def synchronize(self):
        """Prompt the user to synchronize all local files with remote files"""
        
        # download remote versions
        # download remote deleted

        # push deleted remote
        # pull deleted local

        # push new remote
        # pull new local

        # push modified remote
        # pull modified local

        # revert modified remote
        # revert modified local

        # upload local versions to remote
        # upload local delete paths to remote
        
        
        
        return

    def reset_confirm(self):
        """Prompt the user to reset all local and remote synchronization work."""
        return

    def reset_local(self):
        """Remove all modifications made locally by synchronization."""
        return

    def reset_remote(self):
        """Remove all modifications made to the remote repo by synchronization."""
        return

    def _get_randomized_dirname(self):
        """Generate a random string to be used as a file name."""
        return hashlib.md5(bytes(str(dt.datetime.now()), "utf-8")).hexdigest()

    def _hash(self, filepath):
        """Return a unique checksum based on a file's contents."""
        file = open(filepath, "rb")
        data = file.read()
        return hashlib.md5(data).hexdigest()
