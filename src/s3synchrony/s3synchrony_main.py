"""Perform functionality for synchronizing and resetting with a repo.

get_supported_platforms - Get a list of all data platforms supported currently
smart_sync - Synchronizes all local files with a remote repository through user input
reset_all - Completely resets both local and remote repos with user input confirmation

  Typical usage example (for S3):

  import s3synchrony as s3s
  params = {}
  params["datafolder"] = "Data"
  params["aws_bkt"] = "analytics_development"
  params["aws_prfx"] = "S3_Synchrony_Testing"
  if(len(sys.argv) > 1 and sys.argv[1] == "reset"):
      s3s.reset_all(**params)
  else:
      s3s.smart_sync(**params)

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

from jinja2 import TemplateError
import s3synchrony as s3s
import py_starter as ps

def get_Paths_no_init( Dir_inst ):

    Paths_inst = Dir_inst.list_contents_Paths( block_dirs=True, block_paths=False )
    
    for i in range( len(Paths_inst) -1, -1, -1):
        if Paths_inst.Objs[ i ].root == '__init__':
            del Paths_inst.Objs[ i ]

    return Paths_inst

def smart_sync( platform = None, **kwargs):

    """Perform all necessary steps to synchronize a local repository with a remote repo."""

    Platform_inst = get_platform( platform, **kwargs )
    Platform_inst.run()

def reset_all( platform = None, **kwargs ):

    """Reset local and remote directories to original state."""

    Platform_inst = get_platform( platform, **kwargs )
    Platform_inst.reset_all()

def get_supported_platforms():

    platform_Paths = get_Paths_no_init( s3s.platforms_Dir )
    return platform_Paths

def get_platform_Path_user():

    platform_Paths = get_supported_platforms()
    platform_Path = ps.get_selection_from_list( platform_Paths, prompt='Select which platform to use' )

    return platform_Path

def get_platform( platform, **kwargs ):

    platform_Paths = get_supported_platforms()

    # First, see if the given "platform" var is in the supported platforms
    found = False
    for platform_Path in platform_Paths:

        if platform_Path.root == platform:
            found = True
            break
    
    # If not, have the user choose from an existing platform
    if not found:
        platform_Path = get_platform_Path_user()
    
    # If not platform exists at all
    if platform_Path == None:
        return None
    
    else:
        module = platform_Path.import_module()
        return module.Platform( **kwargs )  #return the class point


def get_supported_templates():

    platform_Paths = get_Paths_no_init( s3s.templates_Dir )
    return platform_Paths

def get_template_Path_user():

    template_Paths = get_supported_templates()
    template_Path = ps.get_selection_from_list( template_Paths )

    return template_Path

def get_template( template ):

    """ returns a module """

    template_Paths = get_supported_templates()

    # First, see if the given "platform" var is in the supported platforms
    found = False
    for template_Path in template_Paths:

        if template_Path.root == template:
            found = True
            break
    
    # If not, have the user choose from an existing platform
    if not found:
        template_Path = get_template_Path_user()
    
    # If not platform exists at all
    if template_Path == None:
        return None
    
    else:
        return template_Path.import_module()

def run():

    default_params = {
        'template': None,
        'platform': None
    }

    if s3s.json_Path.exists():
        sync_params = ps.json_to_dict( s3s.json_Path.read() )
        sync_params = ps.merge_dicts( default_params, sync_params )

        template_module = get_template( sync_params['template'] )
        new_sync_params = template_module.get_params( sync_params )

        smart_sync( **new_sync_params )

    else:
        print ()
        print ('ERROR: No sync JSON file. Are you in the right directory?')
        print ('Would you like to place the s3synchrony.json template file in this directory?')

        s3s.template_json_Path.copy( Destination = s3s.json_Path )


if __name__ == '__main__':
    run()

