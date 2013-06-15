class Transaction:
    INPROGRESS = 1
    COMMITTED  = 2
    ABORTED    = 3

    def __init__(self, id, auto_commit=False, state=INPROGRESS):
        self.id = id
        self.auto_commit = auto_commit
        self.state = state
