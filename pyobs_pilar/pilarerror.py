import datetime
import logging


log = logging.getLogger(__name__)


def create_error(ignore: bool = False, reset_max: int = 10, reset_timeout: int = 300,
                 accum_max: int = 5, accum_span: int = 86400) -> dict:
    """Creates an error

    Args:
        ignore:         If true, this error is ignored.
        reset_max:      Maximum number of resets.
        reset_timeout:  Minimum time between errors in seconds.
        accum_max:      Maximum number of errors to occur during accum_span.
        accum_span:     Accumulation time span in seconds.

    Returns:
        (dict) Dictionary containing values
    """
    return {
        'ignore': ignore,
        'reset_max': reset_max,
        'reset_timeout': reset_timeout,
        'accum_max': accum_max,
        'accum_span': accum_span
    }


# List of errors
ERRORS = {
    'ERR_GPS_PositionLost': create_error(ignore=True),
    'ERR_GPS_LeapSecond': create_error(ignore=True),
    'ERR_GPS_TooFewSatellites': create_error(ignore=True),
    'ERR_FilterWheel_RefMismatch': create_error(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Elevation_ETELExecError': create_error(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Oil_TemperatureLow': create_error(ignore=True),
    'ERR_Oil_NoExtractionTimeout': create_error(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Brake_ClosedFromOther': create_error(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Azimuth_ETELError': create_error(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600)
}


class PilarError(object):
    _errors = {}

    def __init__(self, name):
        """Initializes an error

        Args:
            name:           Name of error.
        """
        self._name = name
        self._behaviour = ERRORS[name]
        self._dates = []

    @property
    def name(self):
        return self._name

    @property
    def ignore(self):
        return self._behaviour['ignore']

    @property
    def reset_max(self):
        return self._behaviour['reset_max']

    @property
    def reset_timeout(self):
        return self._behaviour['reset_timeout']

    @property
    def accum_max(self):
        return self._behaviour['accum_max']

    @property
    def accum_span(self):
        return self._behaviour['accum_span']

    def occur(self):
        """Should be called whenever error occurs.

        Returns:
            (bool) Whether or not it is safe to continue.
        """

        # ignore it?
        if self.ignore:
            return True

        # add now to list of dates
        self._dates.append(datetime.datetime.utcnow())

    def fatal(self) -> bool:
        # maximum number reached?
        if len(self._dates) > self.reset_max:
            log.warning('Error %s occurred more often than allowed (%d times).', self._name, len(self._dates))
            return True

        # time between last two errors?
        if len(self._dates) >= 2:
            # calculate difference between last two errors
            diff = (self._dates[-1] - self._dates[-2]).total_seconds()
            # check
            if diff < self.reset_timeout:
                log.warning('Duration (%.0fs) between last two errors of %s was shorter than allowed.',
                                diff, self._name)
                return True

        # collect errors within the last accum_span
        accum_errors = [d for d in self._dates if (self._dates[-1] - d).total_seconds() < self.accum_span]
        if len(accum_errors) > self.accum_max:
            log.warning('Too many (%d) errors of %s occurred during last %d seconds.',
                            len(accum_errors), self._name, self.accum_span)
            return True

        # everything okay
        return False

    @staticmethod
    def create(error_name: str) -> bool:
        # error not found?
        if error_name not in ERRORS:
            log.error('Unknown error "%s" occurred.', error_name)
            return

        # TODO: add more info
        log.warning('Error "%s" occurred.', error_name)

        # did we encounter this error before?
        if error_name not in PilarError._errors:
            # create new one and add to list
            err = PilarError(error_name)
            PilarError._errors[error_name] = err

        # get error and let error occur
        err = PilarError._errors[error_name]
        err.occur()

        # return error
        return err

    @staticmethod
    def check_fatal() -> bool:
        # loop all errors
        for err in PilarError._errors.values():
            if err.fatal():
                return True

        # no fatal condition found
        return False
