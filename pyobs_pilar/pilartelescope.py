import logging
import threading
import time
from threading import Lock

from pyobs.events import FilterChangedEvent
from pyobs.interfaces import IFilters, IFitsHeaderProvider, IFocuser, IFocusModel
from pyobs.modules import timeout
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.threads import LockWithAbort
from .pilardriver import PilarDriver

log = logging.getLogger(__name__)


class PilarTelescope(BaseTelescope, IFilters, IFitsHeaderProvider, IFocuser, IFocusModel):
    def __init__(self, host: str, port: int, username: str, password: str, pilar_fits_headers: dict = None,
                 *args, **kwargs):
        BaseTelescope.__init__(self, thread_funcs=[self._pilar_update, self._focus_tracker], *args, **kwargs)

        # init pilar
        log.info('Connecting to Pilar at %s:%d...', host, port)
        self._pilar = PilarDriver(host, port, username, password)
        self._pilar.open()
        self._pilar.wait_for_login()

        # get list of filters
        self._filters = self._pilar.filters()

        # get Pilar variables for status updates...
        self._pilar_variables = [
            'OBJECT.EQUATORIAL.RA', 'OBJECT.EQUATORIAL.DEC',
            'POSITION.EQUATORIAL.RA_J2000', 'POSITION.EQUATORIAL.DEC_J2000',
            'POSITION.HORIZONTAL.ZD', 'POSITION.HORIZONTAL.ALT', 'POSITION.HORIZONTAL.AZ',
            'POSITION.INSTRUMENTAL.FOCUS.REALPOS',
            'POSITION.INSTRUMENTAL.FILTER[2].CURRPOS',
            'AUXILIARY.SENSOR[1].VALUE', 'AUXILIARY.SENSOR[2].VALUE',
            'AUXILIARY.SENSOR[3].VALUE', 'AUXILIARY.SENSOR[4].VALUE',
            'POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS', 'POINTING.SETUP.DEROTATOR.OFFSET',
            'TELESCOPE.READY_STATE', 'TELESCOPE.MOTION_STATE',
            'POSITION.INSTRUMENTAL.AZ.OFFSET', 'POSITION.INSTRUMENTAL.ZD.OFFSET'
        ]

        # ... and add user defined ones
        self._pilar_fits_headers = pilar_fits_headers
        for var in pilar_fits_headers.keys():
            if var not in self._pilar_variables:
                self._pilar_variables.append(var)

        # offsets
        self._offset_az = 0.
        self._offset_zd = 0.

        # create update thread
        self._status = {}
        self._lock = Lock()

        # optimal focus
        self._last_focus_time = None

        # some multi-threading stuff
        self._lock_focus = threading.Lock()
        self._abort_focus = threading.Event()
        self._lock_filter = threading.Lock()
        self._abort_filter = threading.Event()

    def open(self):
        """Open module."""
        BaseTelescope.open(self)

        # subscribe to events
        if self.comm:
            self.comm.register_event(FilterChangedEvent)

    def close(self):
        BaseTelescope.close(self)

        log.info('Closing connection to Pilar...')
        self._pilar.close()
        log.info('Shutting down...')

    def _pilar_update(self):
        # log
        log.info('Starting Pilar update thread...')

        while not self.closing.is_set():
            # define values to request
            keys = self._pilar_variables

            # get data
            try:
                multi = self._pilar.get_multi(keys)
            except TimeoutError:
                # sleep a little and continue
                log.error('Request to Pilar timed out.')
                self.closing.wait(60)
                continue

            # join status
            try:
                s = {key: float(multi[key]) for key in keys}

                # and set it
                with self._lock:
                    self._status = s

            except ValueError:
                # ignore it
                pass

            # set motion status
            if float(self._status['TELESCOPE.READY_STATE']) == 0.:
                self._change_motion_status(BaseTelescope.Status.PARKED)
            elif 0. < float(self._status['TELESCOPE.READY_STATE']) < 1.:
                self._change_motion_status(BaseTelescope.Status.INITIALIZING)
            elif float(self._status['TELESCOPE.READY_STATE']) < 0.:
                self._change_motion_status(BaseTelescope.Status.ERROR)
            else:
                # telescope is initialized, check motion state
                ms = int(self._status['TELESCOPE.MOTION_STATE'])
                if ms & (1 << 0):
                    # first bit indicates moving
                    self._change_motion_status(BaseTelescope.Status.SLEWING)
                elif ms & (1 << 2):
                    # third bit indicates tracking
                    self._change_motion_status(BaseTelescope.Status.TRACKING)
                else:
                    # otherwise we're idle
                    self._change_motion_status(BaseTelescope.Status.IDLE)

            # sleep a second
            self.closing.wait(1)

        # log
        log.info('Shutting down Pilar update thread...')

    def _focus_tracker(self):
        # log
        log.info('Starting focus tracking thread...')

        while not self.closing.is_set():
            # set focus?
            if self._last_focus_time is not None and time.time() - self._last_focus_time > 600:
                # calculate optimal focus
                focus = self._calc_optimal_focus()
                log.info('Moving focus to %.2fmm according to temperature model.', focus)

                # move it
                self.set_focus(focus)

                # remember now
                self._last_focus_time = time.time()

            # sleep a little
            self.closing.wait(1)

        # log
        log.info('Shutting down focus tracking thread...')

    def get_fits_headers(self, *args, **kwargs) -> dict:
        # get headers from base
        hdr = BaseTelescope.get_fits_headers(self)

        # define values to request
        keys = {
            'TEL-FOCU': ('POSITION.INSTRUMENTAL.FOCUS.REALPOS', 'Focus position [mm]'),
            'TEL-ROT': ('POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS', 'Derotator instrumental position at end [deg]'),
            'DEROTOFF': ('POINTING.SETUP.DEROTATOR.OFFSET', 'Derotator offset [deg]'),
            'AZOFF': ('POSITION.INSTRUMENTAL.AZ.OFFSET', 'Azimuth offset'),
            'ALTOFF': ('POSITION.INSTRUMENTAL.ZD.OFFSET', 'Altitude offset')
        }

        # add ones from config
        for var, h in self._pilar_fits_headers.items():
            keys[h[0]] = (var, h[1])

        # Monet/S: 3=T1, 1=T2
        # Monet/N: 2=T1, 1=T2

        # create dict and add alt and filter
        with self._lock:
            status = self._status.copy()
        for key, var in keys.items():
            if var[0] in status:
                hdr[key] = (status[var[0]], var[1])

        # negative ALTOFF
        hdr['ALTOFF'] = (-hdr['ALTOFF'][0], hdr['ALTOFF'][1])

        # filter
        if 'POSITION.INSTRUMENTAL.FILTER[2].CURRPOS' in status:
            filter_id = status['POSITION.INSTRUMENTAL.FILTER[2].CURRPOS']
            hdr['FILTER'] = (self._pilar.filter_name(filter_id), 'Current filter')

        # return it
        return hdr

    def get_ra_dec(self) -> (float, float):
        """Returns current RA and Dec.

        Returns:
            Tuple of current RA and Dec in degrees.
        """
        with self._lock:
            return self._status['POSITION.EQUATORIAL.RA_J2000'] * 15., self._status['POSITION.EQUATORIAL.DEC_J2000']

    def get_alt_az(self) -> (float, float):
        """Returns current Alt and Az.

        Returns:
            Tuple of current Alt and Az in degrees.
        """
        with self._lock:
            return self._status['POSITION.HORIZONTAL.ALT'], self._status['POSITION.HORIZONTAL.AZ']

    def list_filters(self, *args, **kwargs) -> list:
        """List available filters.

        Returns:
            List of available filters.
        """
        return self._filters

    def get_filter(self, *args, **kwargs) -> str:
        """Get currently set filter.

        Returns:
            Name of currently set filter.
        """
        return self._pilar.filter_name()

    @timeout(20000)
    def set_filter(self, filter_name: str, *args, **kwargs):
        """Set the current filter.

        Args:
            filter_name: Name of filter to set.

        Raises:
            ValueError: If binning could not be set.
            AcquireLockFailed: If current motion could not be aborted.
        """

        # acquire lock
        with LockWithAbort(self._lock_filter, self._abort_filter):
            log.info('Changing filter to %s...', filter_name)
            self._pilar.change_filter(filter_name, abort_event=self._abort_filter)
            log.info('Filter changed.')

            # send event
            self.comm.send_event(FilterChangedEvent(filter_name))

    def _move(self, alt: float, az: float, abort_event: threading.Event):
        """Actually moves to given coordinates. Must be implemented by derived classes.

        Args:
            alt: Alt in deg to move to.
            az: Az in deg to move to.
            abort_event: Event that gets triggered when movement should be aborted.

        Raises:
            Exception: On error.
        """

        # reset offsets
        self.reset_offset()

        # start tracking
        log.info('Starting tracking at Alt=%.2f, Az=%.5f', alt, az)
        success = self._pilar.goto(alt, az, abort_event=abort_event)

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    def _track(self, ra: float, dec: float, abort_event: threading.Event):
        """Actually starts tracking on given coordinates. Must be implemented by derived classes.

        Args:
            ra: RA in deg to track.
            dec: Dec in deg to track.
            abort_event: Event that gets triggered when movement should be aborted.

        Raises:
            Exception: On any error.
        """

        # reset offsets
        self.reset_offset()

        # start tracking
        log.info('Starting tracking at RA=%.5f, Dec=%.5f', ra, dec)
        success = self._pilar.track(ra, dec, abort_event=abort_event)

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    def get_focus(self, *args, **kwargs) -> float:
        """Return current focus.

        Returns:
            Current focus.
        """
        return float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS'))

    @timeout(300000)
    def set_focus(self, focus: float, *args, **kwargs):
        """Sets new focus.

        Args:
            focus: New focus value.

        Raises:
            InterruptedError: If focus was interrupted.
            AcquireLockFailed: If current motion could not be aborted.
        """

        # acquire lock
        with LockWithAbort(self._lock_focus, self._abort_focus):
            # reset optimal focus
            self._last_focus_time = None

            # set absolute focus
            return self._pilar.focus(focus, abort_event=self._abort_focus)

    def _calc_optimal_focus(self):
        # get current M1/M2 temperatures
        t1 = float(self._pilar.get('AUXILIARY.SENSOR[4].VALUE'))
        t2 = float(self._pilar.get('AUXILIARY.SENSOR[1].VALUE'))

        # calculate model
        f0 = 42.170124
        lt1 = -0.731794
        qt1 = 0.020481
        lt2 = 0.715955
        qt2 = -0.022598

        # return optimal focus
        return f0 + lt1 * t1 + qt1 * t1**2 + lt2 * t2 + qt2 * t2**2

    @timeout(300000)
    def set_optimal_focus(self, *args, **kwargs):
        """Sets optimal focus.

        Raises:
            InterruptedError: If focus was interrupted.
            AcquireLockFailed: If current motion could not be aborted.
        """

        # acquire lock
        with LockWithAbort(self._lock_focus, self._abort_focus):
            # calculate model
            focus = self._calc_optimal_focus()
            log.info('Setting optimal focus of %.2fmm and activating focus tracking...', focus)

            # set absolute focus
            self._pilar.focus(focus, abort_event=self._abort_focus)

            # activate optimal focus
            self._last_focus_time = time.time()

    def offset(self, dalt: float, daz: float, *args, **kwargs):
        """Move an Alt/Az offset, which will be reset on next call of track.

        Args:
            dalt: Altitude offset in degrees.
            daz: Azimuth offset in degrees.

        Raises:
            ValueError: If offset could not be set.
        """
        log.info('Moving offset of dAlt=%.3f", dAz=%.3f".', dalt*3600., daz*3600.)

        # get current offsets
        offset_az = float(self._pilar.get('POSITION.INSTRUMENTAL.AZ.OFFSET'))
        offset_zd = float(self._pilar.get('POSITION.INSTRUMENTAL.ZD.OFFSET'))

        # adjust them
        offset_az += daz
        offset_zd -= dalt

        # and set
        self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', offset_az)
        self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', offset_zd)

    def reset_offset(self, *args, **kwargs):
        """Reset Alt/Az offset.

        Raises:
            ValueError: If offset could not be reset.
        """
        self._offset_az = 0.
        self._offset_zd = 0.
        self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', self._offset_az)
        self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', self._offset_zd)

    @timeout(300000)
    def init(self, *args, **kwargs):
        """Initialize telescope.

        Raises:
            ValueError: If telescope could not be initialized.
        """
        if not self._pilar.init():
            raise ValueError('Could not initialize telescope.')

    @timeout(300000)
    def park(self, *args, **kwargs):
        """Park telescope.

        Raises:
            ValueError: If telescope could not be parked.
        """

        # reset all offsets
        self.reset_offset()

        # park telescope
        if not self._pilar.park():
            raise ValueError('Could not park telescope.')
