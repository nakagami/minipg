class Error(Exception):
    def __init__(self, *args):
        super(Error, self).__init__(*args)
        if len(args) > 0:
            self.message = args[0]
        else:
            self.message = b'Database Error'
        if len(args) > 1:
            self.code = args[1]
        else:
            self.code = None

    def __str__(self):
        return self.message

    def __repr__(self):
        if self.code:
            return self.code + ":" + self.message
        return self.message


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DisconnectByPeer(Warning):
    pass


class InternalError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'InternalError')


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'NotSupportedError')
