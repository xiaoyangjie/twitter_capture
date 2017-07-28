# __author__ = "Junwu He"
# __date__ = "May 12, 2016"
import logging


class CrawlerError(Exception):
    """Base class for Crawler exceptions"""
    pass


class CrawlerConnectionError(CrawlerError):
    """Raised when the connection needs to be re-established"""

    def __init__(self, value):
        super(Exception, self).__init__(value)
        logging.warning('%s %s' % (type(value), value))


class CrawlerRequestError(CrawlerError):
    """Raised when request fails"""

    def __init__(self, status_code):
        if status_code >= 500:
            msg = 'Twitter internal error (you may re-try)'
        else:
            msg = 'Twitter request failed'
        logging.warning('Status code %d: %s' % (status_code, msg))
        super(Exception, self).__init__(msg)
        self.status_code = status_code

    def __str__(self):
        return '%s (%d)' % (self.args[0], self.status_code)


class CrawlerEmptyException(CrawlerError):
    def __init__(self):
        self.msg = 'Result is empty'
        logging.warning(self.msg)
        super(Exception, self).__init__(self.msg)

    def __str__(self):
        return self.msg
