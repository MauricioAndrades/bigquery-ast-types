class Scope:
    """
    Tracks name resolution context for SQL nodes (e.g., table aliases, CTEs).
    """
    def __init__(self, parent=None):
        self.parent = parent
        self.bindings = {}

    def declare(self, name, value):
        self.bindings[name] = value

    def lookup(self, name):
        if name in self.bindings:
            return self.bindings[name]
        elif self.parent:
            return self.parent.lookup(name)
        return None

    def get_bindings(self):
        return dict(self.bindings)

    def is_global(self):
        return self.parent is None