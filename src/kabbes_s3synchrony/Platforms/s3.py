import kabbes_s3synchrony
import py_starter as ps
import aws_connections
import dir_ops as do
import os

class Platform( kabbes_s3synchrony.BasePlatform ):

    NAME = do.Path( os.path.abspath( __file__ ) ).root #s3

    DIR_CLASS = aws_connections.s3.S3Dir
    DIRS_CLASS = aws_connections.s3.S3Dirs
    PATH_CLASS = aws_connections.s3.S3Path
    PATHS_CLASS = aws_connections.s3.S3Paths

    def __init__(self, *args, **kwargs ):

        kabbes_s3synchrony.BasePlatform.__init__( self, *args, **kwargs )

        self.data_rDir = aws_connections.s3.S3Dir( bucket = self.cfg['aws_bkt'], path = self.Connection.cfg['remote_data_dir'], conn = self.remote_connection )

        self._util_rDir = self.data_rDir.join_Dir( path = self.UTIL_DIR ) #S3Dir
        self._util_deleted_rDir = self._util_rDir.join_Dir( path = 'deleted' ) #S3Dir
        self._remote_versions_rPath = self._util_rDir.join_Path( path = self._remote_versions_lPath.filename )
        self._remote_delete_rPath = self._util_rDir.join_Path( path = self._remote_delete_lPath.filename )

    def _get_remote_connection( self ):
        self.remote_connection = aws_connections.Client( 
            dict = {
                "boto3_client": self.NAME,
                "connection.kwargs": self.cfg['credentials'].get_raw_dict() 
            } 
        )

