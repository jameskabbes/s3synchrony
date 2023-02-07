import kabbes_client
import kabbes_s3synchrony

class Client( kabbes_s3synchrony.Connection ):

    _BASE_DICT = {
    }

    def __init__( self, dict={} ):

        d = {}
        d.update( Client._BASE_DICT )
        d.update( dict )

        self.Package = kabbes_client.Package( kabbes_s3synchrony._Dir, dict=d )
        self.cfg = self.Package.cfg

        kabbes_s3synchrony.Connection.__init__( self )
