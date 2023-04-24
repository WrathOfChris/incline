from incline.error import InclineError, InclineInterface, InclineNotFound


class InclineRouter(object):

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name=name, region=region)

    def init(self, name, region):
        self.name = name
        self.region = region
        self.delimiter = '|'
        self.route_read = list()
        self.route_write = list()
        self.route_search = list()
        self.default()

    def default(self):
        pass

    def lookup(self, action, kid):
        if action == "read":
            return self.read
        elif action == "write":
            return self.write
        elif action == "search":
            return self.search
        else:
            raise InclineNotFound('router invalid lookup action')

    @property
    def read(self):
        return self.route_read

    @property
    def write(self):
        return self.route_write

    @property
    def search(self):
        return self.route_search


class InclineRouterOne(InclineRouter):

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name=name, region=region)

    def default(self):
        self.route_read = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name)
        ]
        self.route_write = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name)
        ]
        self.route_search = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name)
        ]


class InclineRouterTwo(InclineRouter):

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name=name, region=region)

    def default(self):
        self.route_read = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
        self.route_write = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
        self.route_search = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]


class InclineRouterRead1(InclineRouter):
    """
    Unbalanced test router
    Write 1+2
    Read 1
    """

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name=name, region=region)

    def default(self):
        # XXX FOR TESTING XXX
        self.route_read = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
        ]
        self.route_write = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
        self.route_search = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]


class InclineRouterRead2(InclineRouter):
    """
    Unbalanced test router
    Write 1+2
    Read 2
    """

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name=name, region=region)

    def default(self):
        # XXX FOR TESTING XXX
        self.route_read = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
        self.route_write = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
        self.route_search = [
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '1'),
            'dynamo{0}{1}{2}{3}'.format(self.delimiter, self.region,
                                        self.delimiter, self.name + '2')
        ]
