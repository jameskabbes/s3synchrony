from parent_class import ParentClass
import kabbes_s3synchrony

class Connection( ParentClass ):

    def __init__( self ):
        ParentClass.__init__( self )

        self.template_module = kabbes_s3synchrony.get_template( self.cfg['template'] )
        self.platform_module = kabbes_s3synchrony.get_platform( self.cfg['platform'] )
        self.platform_node_name = 'platforms.' + self.cfg['platform']

    def run( self ):

        self.template_module.set_cfg( self )
        self.platform = self.platform_module.Platform( self )

        self.platform.run()

