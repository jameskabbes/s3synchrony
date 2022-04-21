import s3synchrony
from s3synchrony import smart_sync 
import aws_credentials
import user_profile

def run( sync_params, *sys_args ):

    sync_params['_name'] = user_profile.profile.name

    aws_role = user_profile.profile.aws_roles[ sync_params['aws_role_shorthand'] ]
    sync_params['credentials'] = aws_credentials.Creds[ aws_role ].dict

    print (sync_params)
    smart_sync( **sync_params )
    
