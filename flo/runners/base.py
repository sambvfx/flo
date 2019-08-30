
class Runner(object):

    def __init__(self, **options):
        self.options = options
        self.nodes = []

    def add(self, *nodes):
        for n in nodes:
            if n not in self.nodes:
                self.nodes.append(n)

    def execute(self):
        results = {}
        for node in self.nodes:
            results[node] = node()
