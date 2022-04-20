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

import os
import sys
import pathlib
import shutil

import pandas as pd
import datetime as dt
import boto3
import botocore.exceptions
import pyperclip
import aws_credentials

import s3synchrony as s3s
from s3synchrony import BasePlatform

import py_starter as ps
import dir_ops as do

import aws_connections
import aws_connections.s3 as s3


class Platform( s3s.BasePlatform ):

    util_dir = '.S3'

    DEFAULT_KWARGS = {
    'aws_bkt': None,
    'credentials': {}
    }

    def __init__(self, **kwargs ):

        joined_kwargs = ps.merge_dicts( Platform.DEFAULT_KWARGS, kwargs )
        BasePlatform.__init__( self, **joined_kwargs )

        if not s3.S3Dir.is_Dir( self.data_rDir ):
            self.data_rDir = s3.S3Dir( bucket = self.aws_bkt, path = self.remote_data_dir )

        self._util_rDir = s3.S3Dir( bucket = self.aws_bkt, path = self.data_rDir.join( self.util_dir ) )
        self._remote_versions_rPath = s3.S3Path( bucket = self.aws_bkt, 
                                                  path = self._util_rDir.join( self._remote_versions_lPath.filename ) )
        self._remote_delete_rPath = s3.S3Path( bucket = self.aws_bkt, 
                                                path = self._util_rDir.join( self._remote_delete_lPath.filename ) )


    def _get_remote_connection( self ):

        return aws_credentials.Connection( "s3", **self.credentials )
        subfolders = self.data_rDir.list_subfolders()
        
    def _initialize_util_Dir(self):
        """Check for all necessary files on the S3 prefix for synchronization."""
        
        randhex = self._get_randomized_dirname()
        download_dir_loc = self._tmp_lDir.join( randhex )

        self.data_rDir.download( path = download_dir_loc )
        versions = self._compute_directory( download_dir_loc, False )

        self.data_lDir.create()
        self._util_lDir.create()

        versions.to_csv( download_dir_loc + '/' + self._remote_versions_lPath.filename, index = False )

        empty = pd.DataFrame( columns=self.columns )
        empty.to_csv(self._tmppath + '/' +
                     randhex + "/deletedS3.csv", index=False)

        self.resource.meta.client.upload_file(
            self._tmppath + '/' + randhex + "/versions.csv", self.aws_bkt, self._s3subdirremote + "versions.csv")
        self.resource.meta.client.upload_file(
            self._tmppath + '/' + randhex + "/deletedS3.csv", self.aws_bkt, self._s3subdirremote + "deletedS3.csv")

    def _download_entire_prefix(self, bucket, prefix, local_path):
        """Download the entire contents of an S3 prefix."""
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        try:
            response["Contents"]
        except:
            return

        s3_files = []
        for file_dict in response["Contents"]:
            s3_files.append(file_dict["Key"])

        local_paths = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_paths.append(os.path.join(root, file)[
                    len(local_path)+1:].replace('\\', '/'))

        for i in range(len(s3_files)):
            if(s3_files[i].startswith(prefix)):
                s3_files[i] = s3_files[i][len(prefix + '/')-1:]  # Remove?

        s3_to_download = {}
        for rel_file in s3_files:
            if rel_file not in local_paths:
                if len(os.path.basename(rel_file)) > 0:
                    target_location = os.path.join(prefix, rel_file)
                    s3_to_download[target_location] = os.path.join(
                        local_path, rel_file)

        if len(s3_to_download) > 0:
            for s3_file, local_path in s3_to_download.items():
                self._download_file(bucket, s3_file, local_path)

    def _compute_directory(self, directory, ignoreS3=True):
        """Create a dataframe describing all files in a local directory."""
        df = pd.DataFrame(columns=self.columns)
        files_under_folder = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                files_under_folder.append(os.path.join(root, file)[
                    len(directory)+1:].replace('\\', '/'))

        for file_path in files_under_folder:
            if self._s3id in file_path and ignoreS3:
                continue
            timestamp = dt.datetime.fromtimestamp(pathlib.Path(
                directory + '\\' + file_path).stat().st_mtime).strftime(self.dttm_format)

            new_df = pd.DataFrame(columns=self.columns)
            new_df[self._file_colname] = [file_path]
            new_df[self._time_colname] = timestamp
            new_df[self._hash_colname] = self._hash(
                directory + '\\' + file_path)
            new_df[self._editor_colname] = self._name
            df = pd.concat([df, new_df], ignore_index=True)
        return df

    def _compute_dfs(self, folder_path):
        """Return a list of dfs containing all the information for smart_sync."""
        mine = self._compute_directory(folder_path)
        other = pd.read_csv(self._s3versionspath)
        mine, other = self._filter_ignore(mine, other)
        inboth = mine[mine[self._file_colname].isin(other[self._file_colname])]

        mod_mine = []  # Files more recently modified Locally
        mod_other = []  # Files more recently modified on S3
        for file in inboth[self._file_colname]:
            mycs = inboth.loc[inboth[self._file_colname]
                              == file][self._hash_colname].iloc[0]
            othercs = other.loc[other[self._file_colname]
                                == file][self._hash_colname].iloc[0]
            if(mycs != othercs):
                datemine = dt.datetime.strptime(
                    mine.loc[mine[self._file_colname] == file][self._time_colname].iloc[0], self.dttm_format)
                dateother = dt.datetime.strptime(
                    other.loc[other[self._file_colname] == file][self._time_colname].iloc[0], self.dttm_format)
                if(datemine > dateother):
                    mod_mine.append([file, datemine, dateother])
                else:
                    mod_other.append([file, datemine, dateother])
        return (mine, other, mod_mine, mod_other)

    def _filter_ignore(self, mine, other):
        """Remove all files that should be ignored as requested by the user."""
        mine = mine.loc[~mine[self._file_colname].isin(self._ignore)]
        other = other.loc[~other[self._file_colname].isin(self._ignore)]
        return mine, other

    def _upload_to_s3(self, files):
        """Attempt to upload every file provided to the remote S3 prefix."""
        successful = []
        for file in files:
            print("Uploading " + file + " to S3...")
            try:
                self.resource.meta.client.upload_file(
                    self.datapath + '/' + file, self.aws_bkt, self.aws_prfx + '/' + file)
                successful.append(file)
            except Exception as exc:
                print("ERROR: Couldn't upload." + file)
                print(
                    "Please check the error log for more information. Skipping this file.")
                self._log += "-----------------------------upload_to_s3 error-----------------------------\n"
                self._log += "File: " + self.datapath + '/' + file + '\n'
                self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                self._log += "S3 Key: " + self.aws_prfx + '/' + file + "\n\n"
                self._log += "Line number: " + \
                    str(sys.exc_info()[2].tb_lineno) + '\n'
                self._log += "Message: " + str(exc) + "\n\n\n"
        return successful

    def _download_from_s3(self, files):
        """Attempt to download every file provided from the remote S3 prefix."""
        for file in files:
            print("Downloading " + file + " from S3...")
            try:
                self._download_file(self.aws_bkt, self.aws_prfx + '/' +
                                    file, self.datapath + '/' + file)
            except Exception as exc:
                print("ERROR: Couldn't download " + file)
                print(
                    "Please check the error log for more information. Skipping this file.")
                self._log += "-----------------------------download_from_s3 error-----------------------------\n"
                self._log += "File: " + self.datapath + '/' + file + '\n'
                self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                self._log += "S3 Key: " + self.aws_prfx + '/' + file + "\n\n"
                self._log += "Line number: " + \
                    str(sys.exc_info()[2].tb_lineno) + '\n'
                self._log += "Message: " + str(exc) + "\n\n\n"

    def _delete_from_s3(self, files):
        """Attempt to delete every file provided from the remote S3 prefix."""
        print("Are you sure you want to delete these files from S3?")
        inp = input(", ".join(files) + "\tY/N: ")
        successful = []
        if(inp.lower() in ['y', 'yes']):
            for file in files:
                print("Deleting " + file + " from S3...")
                localFile = file.split('/')[-1]
                downloaded = False
                deleted = False
                reuploaded = False

                try:
                    # Download the file from S3 into our tmp folder, delete it from S3, and
                    # reupload from our tmp folder into the .S3/deleted folder on AWS
                    self._download_file(self.aws_bkt, self.aws_prfx + '/' + file,
                                        self._tmppath + '/' + localFile)
                    downloaded = True
                    self.client.delete_object(
                        Bucket=self.aws_bkt, Key=self.aws_prfx + '/' + file)
                    deleted = True
                    self.resource.meta.client.upload_file(
                        self._tmppath + '/' + localFile, self.aws_bkt, self._s3subdirremote + 'deleted/' + localFile)
                    reuploaded = True
                    successful.append(file)
                except Exception as exc:
                    print("ERROR: Couldn't delete " + file + " from S3.")
                    print(
                        "Please check the error log for more information. Skipping this file.")
                    self._log += "-----------------------------delete_from_s3 error-----------------------------\n"
                    self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                    self._log += "S3 Key: " + self.aws_prfx + '/' + file + '\n'
                    self._log += "File Downloaded To: " + self._tmppath + '/' + localFile + '\n'
                    self._log += "S3 Key Reupload: " + \
                        self._s3subdirremote + "deleted/" + localFile + '\n'
                    self._log += "Downloaded: " + str(downloaded) + '\n'
                    self._log += "Deleted: " + str(deleted) + '\n'
                    self._log += "Reuploaded: " + str(reuploaded) + "\n\n"
                    self._log += "Line number: " + \
                        str(sys.exc_info()[2].tb_lineno) + '\n'
                    self._log += "Message: " + str(exc) + "\n\n\n"
        return successful

    def _delete_from_local(self, files):
        """Attempt to delete every file requested from our local data folder."""
        print("Are you sure you want to delete these files from your computer?")
        inp = input(", ".join(files) + '\tY/N: ')
        if(inp.lower() in ['y', 'yes']):
            for file in files:
                try:
                    os.remove(self.datapath + '/' + file)
                except Exception as exc:
                    print("ERROR: Couldn't delete " +
                          file + " from your computer.")
                    print(
                        "Please check the error log for more information. Skipping this file.")
                    self._log += "-----------------------------delete_from_local error-----------------------------\n"
                    self._log += "File: " + self.datapath + '/' + file + "\n\n"
                    self._log += "Line number: " + \
                        str(sys.exc_info()[2].tb_lineno) + '\n'
                    self._log += "Message: " + str(exc) + "\n\n\n"

    def _apply_selected_indices(self, data_function, allfiles):
        """Prompt the user to select certain files to perform a synchronization function on."""
        indicies = input(
            "\n(enter numbers separated by commas, enter for cancel, 'all' for all):")
        selectedfiles = []
        if indicies.lower() in ['a', "all"]:
            selectedfiles = data_function(allfiles)
        elif indicies != "":
            indicies = set(indicies.split(','))
            for num in indicies:
                num = int(num)
                if num >= 0 and num < len(allfiles):
                    selectedfiles.append(allfiles[num])
            selectedfiles = data_function(selectedfiles)
        return selectedfiles

    def _push_sequence(self, listfiles, mine, other):
        """User-prompted uploading of files from a dataframe."""
        to_push = []
        index = 0
        for file in listfiles:
            to_push.append(file[0])
            print(index, file[0], '\t', file[1], '\t', file[2], "\t by",
                  other.loc[other[self._file_colname] == file[0]][self._editor_colname].iloc[0])
            index += 1
        selectedpush = self._apply_selected_indices(
            self._upload_to_s3, to_push)

        updatedins3 = mine.loc[mine[self._file_colname].isin(
            selectedpush)]
        newversions = pd.concat([other, updatedins3])
        newversions = newversions.drop_duplicates(
            [self._file_colname], keep="last").sort_index()
        newversions.to_csv(self._s3versionspath, index=False)
        print("Done.\n")

    def _push_modified_s3(self):
        """Update files on S3 with modifications that were made locally more recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)
        if(len(mod_mine) > 0):
            print(
                "UPLOAD: Would you like to update these files on S3 with your local changes?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._push_sequence(mod_mine, mine, other)

    def _revert_modified_s3(self):
        """Revert files on S3 with modifications that were made locally less recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)
        if(len(mod_other) > 0):
            print(
                "UPLOAD: Would you like to revert these files on S3 back to your local versions?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._push_sequence(mod_other, mine, other)

    def _pull_sequence(self, listfiles, other):
        """User-prompted downloading of files from a dataframe."""
        to_pull = []
        index = 0
        for file in listfiles:
            to_pull.append(file[0])
            print(index, file[0], '\t', file[1], '\t', file[2], "\t by",
                  other.loc[other[self._file_colname] == file[0]][self._editor_colname].iloc[0])
            index += 1
        self._apply_selected_indices(self._download_from_s3, to_pull)
        print("Done.\n")

    def _pull_modified_local(self):
        """Update local files with modifications that were made on S3 more recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)
        if(len(mod_other) > 0):
            print(
                "DOWNLOAD: Would you like to update these local files with the changes from S3?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._pull_sequence(mod_other, other)

    def _revert_modified_local(self):
        """Revert local files with modifications that were made on S3 less recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)
        if(len(mod_mine) > 0):
            print(
                "DOWNLOAD: Would you like to revert these local files back to the versions on S3?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._pull_sequence(mod_mine, other)

    def _push_new_s3(self):
        """Upload files to S3 that were created locally."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)

        # Find files that are in our directory but not AWS, and load in files deleted from AWS
        new_local = mine.loc[~mine[self._file_colname].isin(
            other[self._file_colname])]
        deletedfiles = pd.read_csv(self._s3delpath)[
            self._file_colname].values.tolist()

        if(len(new_local) > 0):
            print(
                "UPLOAD: Would you like to upload these new files to S3 that were created locally?:")
            print("('file name' / 'Date last modified Locally')\n")
            to_add = []
            index = 0
            for i, row in new_local.iterrows():
                to_add.append(row[self._file_colname])
                print(index, row[self._file_colname],
                      '\t', row[self._time_colname], end='\t')
                if(row[self._file_colname] in deletedfiles):
                    print("*DELETED ON S3", end='')
                print()
                index += 1

            selectedadd = self._apply_selected_indices(
                self._upload_to_s3, to_add)

            added_to_s3 = mine.loc[mine[self._file_colname].isin(selectedadd)]
            newversions = pd.concat([other, added_to_s3])
            newversions.to_csv(self._s3versionspath, index=False)
            print("Done.\n")

    def _push_deleted_s3(self):
        """Remove files from S3 that were deleted locally."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)

        # Load in what files we had last time, and what files we have deleted in the past
        oldmine = pd.read_csv(self._localversionspath)
        deletedlocal = pd.read_csv(self._localdelpath)
        # Combine a list of files we have deleted and files we have had in the past, remove any duplicates
        deletedlocal = pd.concat([oldmine, deletedlocal])
        deletedlocal = deletedlocal.drop_duplicates(
            [self._file_colname], keep="last")
        # From previous files + deleted files select only the ones that AREN'T in our local system but ARE on AWS
        deletedlocal = deletedlocal[~deletedlocal[self._file_colname].isin(
            mine[self._file_colname])]
        deletedlocal = other[other[self._file_colname].isin(
            deletedlocal[self._file_colname])]

        deletedlocal.to_csv(self._localdelpath, index=False)

        if(len(deletedlocal) > 0):
            print(
                "UPLOAD: Would you like to delete these files on S3 that were deleted locally?:")
            print("('file name' / 'Date last modified on S3')\n")
            to_delete = []
            index = 0
            for i, row in deletedlocal.iterrows():
                to_delete.append(row[self._file_colname])
                print(index, row[self._file_colname], '\t',
                      row[self._time_colname], '\t by', row[self._editor_colname])
                index += 1

            selecteddelete = self._apply_selected_indices(
                self._delete_from_s3, to_delete)

            deleteds3 = pd.read_csv(self._s3delpath)
            newdeleted = other.loc[other[self._file_colname].isin(
                selecteddelete)]
            deleteds3 = pd.concat([deleteds3, newdeleted])
            deleteds3.to_csv(self._s3delpath)

            # Replace any removed file names with N/A and then drop if they have been deleted
            other[self._file_colname] = other[self._file_colname].where(
                ~other[self._file_colname].isin(selecteddelete))
            other = other.dropna()
            other.to_csv(self._s3versionspath, index=False)
            print("Done.\n")

    def _pull_new_local(self):
        """Download files from S3 that were created recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)

        # Find files that are on S3 but not our local system and read in files we have deleted locally
        news3 = other.loc[~other[self._file_colname].isin(
            mine[self._file_colname])]
        deletedfiles = pd.read_csv(self._localdelpath)[
            self._file_colname].values.tolist()

        if(len(news3) > 0):
            print(
                "DOWNLOAD: Would you like to download these new files that were created on S3?:")
            print("('file name' / 'Date last modified on S3')\n")
            to_download = []
            index = 0
            for i, row in news3.iterrows():
                to_download.append(row[self._file_colname])
                print(index, row[self._file_colname], '\t', row[self._time_colname],
                      "\t by", row[self._editor_colname], end='\t')
                if(row[self._file_colname] in deletedfiles):
                    print("*DELETED LOCALLY", end='')
                print()
                index += 1

            self._apply_selected_indices(self._download_from_s3, to_download)
            print("Done.\n")

    def _pull_deleted_local(self):
        """Remove files from local system that were deleted on S3."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datapath)

        # Load in files deleted from S3, and select only those that ARE on our local system and AREN'T on AWS
        deleteds3 = pd.read_csv(self._s3delpath)
        deleteds3 = deleteds3[deleteds3[self._file_colname].isin(
            mine[self._file_colname])]
        deleteds3 = deleteds3[~deleteds3[self._file_colname].isin(
            other[self._file_colname])]

        if(len(deleteds3) > 0):
            print(
                "DOWNLOAD: Would you like to delete these files from your computer that were deleted on S3?:")
            print("('file name' / 'Date last modified locally')\n")
            to_delete = []
            index = 0
            for i, row in deleteds3.iterrows():
                to_delete.append(row[self._file_colname])
                mask = row[self._file_colname] == mine[self._file_colname]
                print(index, row[self._file_colname], '\t',
                      mine.loc[mask][self._file_colname].iloc[0])
                index += 1

            self._apply_selected_indices(self._delete_from_local, to_delete)
            print('Done.\n')
