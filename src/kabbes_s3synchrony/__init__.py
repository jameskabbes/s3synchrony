import dir_ops as do
import os

_Dir = do.Dir( os.path.abspath( __file__ ) ).ascend()   #Dir that contains the package 
templates_Dir = do.Dir( _Dir.join( 'Templates' ) )
platforms_Dir = do.Dir( _Dir.join( 'Platforms') )

from .BasePlatform import BasePlatform
from . import Platforms
def get_platform( platform_name: str ):
    return getattr( Platforms, platform_name )

from . import Templates
def get_template( template_name: str ):
    return getattr( Templates, template_name )

from .Connection import Connection
from .Client import Client