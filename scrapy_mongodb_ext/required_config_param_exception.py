
class RequiredConfigParam(Exception):
    def __init__(self, config_param_name):
        self.config_param_name = config_param_name
    def __str__(self):
        return 'The configuration parameter {0} is required'.format(self.config_param_name)
