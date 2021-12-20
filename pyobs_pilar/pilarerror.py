from __future__ import annotations

import datetime
import logging
from typing import Dict, NamedTuple, Optional, List

log = logging.getLogger(__name__)


class ErrorBehaviour(NamedTuple):
    ignore: bool = False
    reset_max: int = 10
    reset_timeout: int = 300
    accum_max: int = 5
    accum_span: int = 86400


# List of errors
ERRORS = {
    'ERR_GPS_PositionLost': ErrorBehaviour(ignore=True),
    'ERR_GPS_LeapSecond': ErrorBehaviour(ignore=True),
    'ERR_GPS_TooFewSatellites': ErrorBehaviour(ignore=True),
    'ERR_FilterWheel_RefMismatch': ErrorBehaviour(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Elevation_ETELExecError': ErrorBehaviour(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Oil_TemperatureLow': ErrorBehaviour(ignore=True),
    'ERR_Oil_NoExtractionTimeout': ErrorBehaviour(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Brake_ClosedFromOther': ErrorBehaviour(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600),
    'ERR_Azimuth_ETELError': ErrorBehaviour(ignore=False, reset_timeout=0, accum_max=5, accum_span=3600)
}


class PilarError(object):
    _errors: Dict[str, PilarError] = {}

    def __init__(self, name: str):
        """Initializes an error

        Args:
            name:           Name of error.
        """
        self._name = name
        self._behaviour = ERRORS[name]
        self._dates: List[datetime.datetime] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def ignore(self) -> bool:
        return self._behaviour.ignore

    @property
    def reset_max(self) -> int:
        return self._behaviour.reset_max

    @property
    def reset_timeout(self) -> float:
        return self._behaviour.reset_timeout

    @property
    def accum_max(self) -> int:
        return self._behaviour.accum_max

    @property
    def accum_span(self) -> float:
        return self._behaviour.accum_span

    def occur(self) -> bool:
        """Should be called whenever error occurs.

        Returns:
            (bool) Whether or not it is safe to continue.
        """

        # ignore it?
        if self.ignore:
            return True

        # add now to list of dates
        self._dates.append(datetime.datetime.utcnow())
        return True

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
    def create(error_name: str) -> Optional[PilarError]:
        # error not found?
        if error_name not in ERRORS:
            log.error('Unknown error "%s" occurred.', error_name)
            return None

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
