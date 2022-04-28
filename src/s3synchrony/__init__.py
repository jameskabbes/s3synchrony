import dir_ops as do
import os

_Dir = do.Dir( os.path.abspath( __file__ ) ).ascend()   #Dir that contains the package 
_src_Dir = _Dir.ascend()                                  #src Dir that is one above
_repo_Dir = _src_Dir.ascend()                    
_cwd_Dir = do.Dir( do.get_cwd() )

json_Path = do.Path( _cwd_Dir.join( 's3synchrony.json' ) )
templates_Dir = do.Dir( _Dir.join( 'Templates' ) )
platforms_Dir = do.Dir( _Dir.join( 'Platforms') )

template_json_Path = do.Path( _Dir.join( 's3synchrony_template.json' ) )

from .BasePlatform import BasePlatform
from . import Platforms
from . import Templates
from .s3synchrony_main import *
