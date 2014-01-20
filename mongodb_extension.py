
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference

from scrapy import log


VERSION = '0.0.0'

class MongoDBExtension():
    """ MongoDB extension class
        The main reason of this class is to be extended to create
        extensions (middlewares or pipelines as well) which use
        a MongoDB connection to perform their operations
        The objects of this class have a class attribute,
        `configuration` (commented below) and an instance attribute
        named db_connection which is the connection object to  MongoDB
        specified in the settings
    """
    """
    Return the default settings tuples collection
    Each tuple define a configuration parameters, some
    of them are applied straightaway to create MongoClient.
    The elements of each tuple specify:
        1. Parameter name used by this class or object instances.
        2. Settings parameter name. The parameter's name use in
         scrapy settings file
        3. If it is required
        4. If it isn't required, so 3rd element of the tupe is
         False, then  this element exists and contains the
         default value to use
    """
    configurations_definition = (
        ('uri', 'MONGODB_URI', True),
        ('fsync', 'MONGODB_FSYNC', False, False),
        ('write_concern', 'MONGODB_REPLICA_SET_W', False, 0),
        ('database', 'MONGODB_DATABASE', True),
        ('replica_set', 'MONGODB_REPLICA_SET', False, None)
    )

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        """ Constructor """
        config = configure()

        if config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                config['uri'],
                replicaSet=config['replica_set'],
                w=config['write_concern'],
                fsync=config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                config['uri'],
                fsync=config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection
        self.db_connection = connection[config['database']]
        log.msg('Connected to MongoDB {0} and "{1}" database'.format(
            config['uri'],
            config['database']))

    def configure(self, settings):
        """ Configure the MongoDB connection
            checking the required settings and populating the
            optional setting to specified default values
        """
        for parameter in self.configurations_definition:
            key, setting, required = parameter
            if _is_set(self.settings[setting]):
                config[key] = self.settings[setting]
            else:
                if (required):
                    raise RequiredConfigParam(key)
                else:
                    config[key] = parameter[3]

        return config


    def process_item(self, item, spider):
        """ Process the item and add it to MongoDB
        @param item: Item object to put into MongoDB
        @param spider: BaseSpider object which running the queries
        @return The Item object to be processed by following middlewares
            It is required by Scrapy if it should be processed by them
        """

    @classmethod
    def _is_set(string):
        """ Check if a string is None or ''

        :returns: bool - True if the string is empty
        """
        if string is None:
            return True
        elif string == '':
            return True
        return False
