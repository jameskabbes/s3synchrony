import s3synchrony
from s3synchrony import smart_sync 
import aws_credentials
import user_profile

def run( sync_params, *sys_args ):


    sync_params['_name'] = user_profile.profile.name
    sync_params['datapath'] = s3synchrony._cwd_Dir.join(  sync_params['datafolder'] )

    aws_role = user_profile.profile.aws_roles[ sync_params['aws_role_shorthand'] ]
    sync_params['credentials'] = aws_credentials.Creds[ aws_role ].dict

    if can_run( sync_params ):

        print (sync_params)

        smart_sync( **sync_params )
    

def can_run( sync_params ):

    keys = [
        '_name',
        'aws_bkt',
        'aws_prfx',
        'datapath',
        'credentials'
    ]

    not_found = []
    for key in keys:
        if key not in sync_params:
            not_found.append(key)

    if len(not_found) > 0:
        print ('Cannot complete s3synchrony')
        print ('Missing these keys: ')
        print (not_found)
        return False
    
    return True
