import aws_credentials
import user_profile

def get_params( sync_params, *sys_args ):

    sync_params['_name'] = user_profile.profile.name

    aws_role = user_profile.profile.aws_roles[ sync_params['aws_role_shorthand'] ]
    sync_params['credentials'] = aws_credentials.Creds[ aws_role ].dict
  
    return sync_params
