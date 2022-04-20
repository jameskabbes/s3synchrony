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
import pandas as pd
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

        ###
        if not do.Dir.is_Dir( self.data_lDir ):
            self.data_lDir = do.Dir( s3s._cwd_Dir.join( self.local_data_rel_dir ) )

        #lDir is a local Dir, rDir is a remote dir
        self._util_lDir =  do.Dir( self.data_lDir.join(  self.util_dir ) )

        self._remote_versions_lPath = do.Path( self._util_lDir.join( 'versions_remote.csv' ) )
        self._local_versions_lPath =  do.Path( self._util_lDir.join( 'versions_local.csv' ) )

        self._remote_delete_lPath = do.Path( self._util_lDir.join( 'deleted_remote.csv' ) )
        self._local_delete_lPath = do.Path( self._util_lDir.join( 'deleted_local.csv' ) )

        self._tmp_lDir = do.Dir( self._util_lDir.join('tmp') )
        self._logs_lDir = do.Dir( self._util_lDir.join('logs') )
        self._ignore_lPath = do.Path( self._util_lDir.join( 'ignore_remote.txt' ) )

        self._ignore = []

        log_filename = dt.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_") + self._get_randomized_dirname()[:10] + '.txt'
        self._log_lPath = do.Path( self._logs_lDir.join( log_filename ) )

        self._log = ""
        self._reset_approved = False

        ### Get remote connection
        self._get_remote_connection()    


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
        subfolders = self.data_rDir.list_subfolders()
        
        # Check if there is a .S3 subfolder in this S3 bucket/prefix
        if(self._util_rDir.path not in subfolders):
            print("This S3 prefix has not been initialized for S3 Synchrony - Initializing prefix and uploading to S3...")
            self._initialize_util_Dir()
            print("Done.\n")

        has_local_lPaths =  self._local_versions_lPath.exists() and self._local_delete_lPath.exists()
        has_remote_lPaths = self._remote_versions_lPath.exists() and self._remote_delete_lPath.exists()

        if(not has_local_lPaths or not has_remote_lPaths):
            print( "Your data folder has not been initialized for S3 Synchrony - Downloading from S3..." )

            self._download_entire_prefix( self.aws_bkt, self._s3subdirremote, self._s3subdirlocal )

            empty = pd.DataFrame( columns=self.columns )
            empty.to_csv( self._local_delete_lPath.path,      index=False)
            empty.to_csv( self._local_versions_lPath.path ,   index=False)
            
        self._tmp_lDir.create()
        self._logs_lDir.create()
        self._ignore_lPath.create()

        self._ignore = self._ignore_lPath.read().strip().split( '\n' ) #list of lines
        self.write_log()

    def write_log( self ):

        self._log_lPath.write( string = self._log )

    def _connect_to_remote( self ):

        pass

    def synchronize(self):
        """Prompt the user to synchronize all local files with remote files"""
        
        self._remote_versions_rPath.download( Path = self._remote_versions_lPath )
        self._remote_delete_rPath.download( Path = self._remote_delete_lPath )

        self._push_deleted_remote()
        self._pull_deleted_remote()

        self._push_deleted_remote()
        self._pull_deleted_local()

        self._push_new_remote()
        self._pull_new_local()

        self._push_modified_remote()
        self._pull_modified_local()

        self._revert_modified_remote()
        self._revert_modified_local()

        self._remote_versions_rPath.upload( Path = self._remote_versions_lPath )
        self._remote_delete_rPath.upload( Path = self._remote_delete_lPath )

        # Save a snapshot of our current files into versionsLocal for next time
        self._compute_directory( self.data_lDir.path ).to_csv( self._local_versions_lPath.path, index=False )

        self.write_log()
        if self._log == '':
            self._log_lPath.remove( override = True )
        
    def reset_confirm(self) -> bool:

        """Prompt the user to confirm whether a reset can occur."""

        print('Are you sure you would like to reset the remote data directory?')
        print('This will not change any of your file contents, but will delete the entire')
        confirm = input( str(self.util_dir) + ' folder on your local computer and on your AWS prefix: ' + self.aws_prfx + ' (y/n): ')

        if confirm.lower() != 'y':
            print("Reset aborted.")
            self._reset_approved = False

        self._reset_approved = True
        return self._reset_approved

    def reset_local(self):

        """Remove all modifications made locally by synchronization."""

        if self._reset_approved:
            self._util_lDir.remove( override = True )
        else:
            print("Cannot reset local -- user has not approved.")


    def reset_remote(self):
        """Remove all modifications made to the remote repo by synchronization."""
        
        if self._reset_approved:
            self._util_rDir.remove( override = True )
        else:
            print("Cannot reset remote -- user has not approved.")

    def _get_randomized_dirname(self):
        """Generate a random string to be used as a file name."""
        return hashlib.md5(bytes(str(dt.datetime.now()), "utf-8")).hexdigest()

    def _hash(self, filepath):
        """Return a unique checksum based on a file's contents."""
        file = open(filepath, "rb")
        data = file.read()
        return hashlib.md5(data).hexdigest()
