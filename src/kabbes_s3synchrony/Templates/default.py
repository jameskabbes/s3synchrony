import aws_credentials
import user_profile

def set_cfg( Connection ):

    platform_Node = Connection.cfg.get_node( Connection.platform_node_name )

    if Connection.cfg['platform'] == 's3':
        aws_role = user_profile.profile['aws_roles'][ platform_Node[ 'aws_role_shorthand'] ]

        Connection.cfg.load_dict( {'_name': user_profile.profile['name']} )
        platform_Node.load_dict( {
            "credentials": aws_credentials.client.Creds[ aws_role ].dict
        } )

