import logging
from threading import RLock, Event

from pyobs.mixins import FitsNamespaceMixin

from pyobs.events import FilterChangedEvent, InitializedEvent, TelescopeMovingEvent
from pyobs.interfaces import IFilters, IFitsHeaderProvider, IFocuser, ITemperatures, IAltAzOffsets, IMotion
from pyobs.modules import timeout
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.threads import LockWithAbort
from .pilardriver import PilarDriver

log = logging.getLogger(__name__)


class PilarTelescope(BaseTelescope, IAltAzOffsets, IFilters, IFitsHeaderProvider, IFocuser, ITemperatures,
                     FitsNamespaceMixin):
    def __init__(self, host: str, port: int, username: str, password: str, pilar_fits_headers: dict = None,
                 temperatures: dict = None, force_filter_forward: bool = True, *args, **kwargs):
        BaseTelescope.__init__(self, *args, **kwargs, motion_status_interfaces=['ITelescope', 'IFilters', 'IFocuser'])

        # add thread func
        self._add_thread_func(self._pilar_update, True)

        # init pilar
        log.info('Connecting to Pilar at %s:%d...', host, port)
        self._pilar = PilarDriver(host, port, username, password)
        self._pilar.open()
        self._pilar.wait_for_login()

        # get list of filters
        self._filters = self._pilar.filters()
        self._force_filter_forward = force_filter_forward

        # get Pilar variables for status updates...
        self._pilar_variables = [
            'OBJECT.EQUATORIAL.RA', 'OBJECT.EQUATORIAL.DEC',
            'POSITION.EQUATORIAL.RA_J2000', 'POSITION.EQUATORIAL.DEC_J2000',
            'POSITION.HORIZONTAL.ZD', 'POSITION.HORIZONTAL.ALT', 'POSITION.HORIZONTAL.AZ',
            'POSITION.INSTRUMENTAL.FOCUS.REALPOS',
            'POSITION.INSTRUMENTAL.FILTER[2].CURRPOS',
            'POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS', 'POINTING.SETUP.DEROTATOR.OFFSET',
            'TELESCOPE.READY_STATE', 'TELESCOPE.MOTION_STATE',
            'POSITION.INSTRUMENTAL.AZ.OFFSET', 'POSITION.INSTRUMENTAL.ZD.OFFSET'
        ]

        # ... and add user defined ones
        self._pilar_fits_headers = pilar_fits_headers if pilar_fits_headers else {}
        for var in self._pilar_fits_headers.keys():
            if var not in self._pilar_variables:
                self._pilar_variables.append(var)

        # ... and temperatures
        self._temperatures = temperatures if temperatures else {}
        for var in self._temperatures.values():
            if var not in self._pilar_variables:
                self._pilar_variables.append(var)

        # create update thread
        self._status = {}
        self._lock = RLock()

        # optimal focus
        self._last_focus_time = None

        # some multi-threading stuff
        self._lock_focus = RLock()
        self._abort_focus = Event()
        self._lock_filter = RLock()
        self._abort_filter = Event()

        # mixins
        FitsNamespaceMixin.__init__(self, *args, **kwargs)

    def open(self):
        """Open module."""
        BaseTelescope.open(self)

        # subscribe to events
        if self.comm:
            self.comm.register_event(FilterChangedEvent)
            self.comm.register_event(InitializedEvent)

    def close(self):
        BaseTelescope.close(self)

        log.info('Closing connection to Pilar...')
        self._pilar.close()
        log.info('Shutting down...')

    def _pilar_update(self):
        # log
        log.info('Starting Pilar update thread...')

        while not self.closing.is_set():
            # catch everything
            try:
                # do nothing on error
                if self._pilar.has_error:
                    self.closing.wait(10)
                    continue

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

                # acquire lock
                if not self._lock.acquire(blocking=True, timeout=5):
                    log.error('Could not acquire lock.')
                    self.closing.wait(10)
                    continue

                # set status
                self._status = {}
                try:
                    for key in keys:
                        try:
                            self._status[key] = float(multi[key])
                        except ValueError:
                            log.exception('An error has occurred.')
                finally:
                    self._lock.release()

                # set motion status
                # we always set PARKED, INITIALIZING, ERROR, the others only on init
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
                        if self.get_motion_status() == BaseTelescope.Status.UNKNOWN:
                            self._change_motion_status(BaseTelescope.Status.SLEWING)
                    elif ms & (1 << 2):
                        # third bit indicates tracking
                        if self.get_motion_status() == BaseTelescope.Status.UNKNOWN:
                            self._change_motion_status(BaseTelescope.Status.TRACKING)
                    else:
                        # otherwise we're idle
                        if self.get_motion_status() == BaseTelescope.Status.UNKNOWN:
                            self._change_motion_status(BaseTelescope.Status.IDLE)

                # sleep a second
                self.closing.wait(1)

            except:
                log.exception('An unexpected error occured.')
                self.closing.wait(10)

        # log
        log.info('Shutting down Pilar update thread...')

    def get_fits_headers(self, namespaces: list = None, *args, **kwargs) -> dict:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

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
        return self._filter_fits_namespace(hdr, namespaces, **kwargs)

    def get_radec(self, *args, **kwargs) -> (float, float):
        """Returns current RA and Dec.

        Returns:
            Tuple of current RA and Dec in degrees.
        """

        # check error
        if self._pilar.has_error:
            return None, None

        # get RA/Dec
        with self._lock:
            return self._status['POSITION.EQUATORIAL.RA_J2000'] * 15., self._status['POSITION.EQUATORIAL.DEC_J2000']

    def get_altaz(self, *args, **kwargs) -> (float, float):
        """Returns current Alt and Az.

        Returns:
            Tuple of current Alt and Az in degrees.
        """

        # check error
        if self._pilar.has_error:
            return None, None

        # get Alt/Az
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

    @timeout(60000)
    def set_filter(self, filter_name: str, *args, **kwargs):
        """Set the current filter.

        Args:
            filter_name: Name of filter to set.

        Raises:
            ValueError: If binning could not be set.
            AcquireLockFailed: If current motion could not be aborted.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # acquire lock
        with LockWithAbort(self._lock_filter, self._abort_filter):
            log.info('Changing filter to %s...', filter_name)
            self._change_motion_status(IMotion.Status.SLEWING, interface='IFilters')
            self._pilar.change_filter(filter_name, force_forward=self._force_filter_forward,
                                      abort_event=self._abort_filter)
            self._change_motion_status(IMotion.Status.POSITIONED, interface='IFilters')
            log.info('Filter changed.')

            # send event
            self.comm.send_event(FilterChangedEvent(filter_name))

    def _move_altaz(self, alt: float, az: float, abort_event: Event):
        """Actually moves to given coordinates. Must be implemented by derived classes.

        Args:
            alt: Alt in deg to move to.
            az: Az in deg to move to.
            abort_event: Event that gets triggered when movement should be aborted.

        Raises:
            Exception: On error.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # reset offsets
        self._reset_offsets()

        # send event
        self.comm.send_event(TelescopeMovingEvent(alt=alt, az=az))

        # start tracking
        self._change_motion_status(IMotion.Status.SLEWING, interface='ITelescope')
        success = self._pilar.goto(alt, az, abort_event=abort_event)
        self._change_motion_status(IMotion.Status.POSITIONED, interface='ITelescope')

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    def _move_radec(self, ra: float, dec: float, abort_event: Event):
        """Actually starts tracking on given coordinates. Must be implemented by derived classes.

        Args:
            ra: RA in deg to track.
            dec: Dec in deg to track.
            abort_event: Event that gets triggered when movement should be aborted.

        Raises:
            Exception: On any error.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # reset offsets
        self._reset_offsets()

        # send event
        self.comm.send_event(TelescopeMovingEvent(ra=ra, dec=dec))

        # start tracking
        self._change_motion_status(IMotion.Status.SLEWING, interface='ITelescope')
        success = self._pilar.track(ra, dec, abort_event=abort_event)
        self._change_motion_status(IMotion.Status.TRACKING, interface='ITelescope')

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    def _reset_offsets(self):
        """Reset Alt/Az offsets."""
        self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', 0)
        self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', 0)

    def get_focus(self, *args, **kwargs) -> float:
        """Return current focus.

        Returns:
            Current focus.
        """
        return float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.CURRPOS'))

    def get_focus_offset(self, *args, **kwargs) -> float:
        """Return current focus offset.

        Returns:
            Current focus offset.
        """
        return float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET'))

    @timeout(30000)
    def set_focus(self, focus: float, *args, **kwargs):
        """Sets new focus.

        Args:
            focus: New focus value.

        Raises:
            InterruptedError: If focus was interrupted.
            AcquireLockFailed: If current motion could not be aborted.
            TimeoutError: If focus could not be set in given time.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # acquire lock
        with LockWithAbort(self._lock_focus, self._abort_focus):
            # start
            log.info('Setting focus to %.4f...', focus)
            self._change_motion_status(IMotion.Status.SLEWING, interface='IFocuser')
            #self._pilar.set('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS', focus,
            #                timeout=30000, abort_event=self._abort_focus)

            # set focus
            self._pilar.focus(focus)

            # finished
            self._change_motion_status(IMotion.Status.POSITIONED, interface='IFocuser')
            log.info('Reached new focus of %.4f.', float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.CURRPOS')))

    @timeout(30000)
    def set_focus_offset(self, offset: float, *args, **kwargs):
        """Sets focus offset.

        Args:
            offset: New focus offset.

        Raises:
            InterruptedError: If focus was interrupted.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # acquire lock
        with LockWithAbort(self._lock_focus, self._abort_focus):
            # set focus
            log.info('Setting focus offset to %.2f...', offset)
            self._change_motion_status(IMotion.Status.SLEWING, interface='IFocuser')
            self._pilar.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', offset,
                            timeout=10000, abort_event=self._abort_focus)
            self._change_motion_status(IMotion.Status.POSITIONED, interface='IFocuser')
            log.info('Reached new focus offset of %.2f.', float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET')))

    @timeout(10000)
    def set_altaz_offsets(self, dalt: float, daz: float, *args, **kwargs):
        """Move an Alt/Az offset, which will be reset on next call of track.

        Args:
            dalt: Altitude offset in degrees.
            daz: Azimuth offset in degrees.

        Raises:
            ValueError: If offset could not be set.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # set offsets
        log.info('Moving offset of dAlt=%.3f", dAz=%.3f".', dalt * 3600., daz * 3600.)
        old_status = self.get_motion_status(interface='ITelescope')
        self._change_motion_status(IMotion.Status.SLEWING, interface='ITelescope')
        self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', -dalt)
        self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', daz)

        # just wait a second and finish
        self.closing.wait(1)
        self._change_motion_status(old_status, interface='ITelescope')

    def get_altaz_offsets(self, *args, **kwargs) -> (float, float):
        """Get Alt/Az offset.

        Returns:
            Tuple with alt and az offsets.
        """

        # get current offsets and return then
        dalt = -float(self._pilar.get('POSITION.INSTRUMENTAL.ZD.OFFSET'))
        daz = float(self._pilar.get('POSITION.INSTRUMENTAL.AZ.OFFSET'))
        return dalt, daz

    @timeout(300000)
    def init(self, *args, **kwargs):
        """Initialize telescope.

        Raises:
            ValueError: If telescope could not be initialized.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # init telescope
        log.info('Initializing telescope...')
        self._change_motion_status(IMotion.Status.INITIALIZING)
        if not self._pilar.init():
            self._change_motion_status(IMotion.Status.ERROR)
            raise ValueError('Could not initialize telescope.')

        # init filter wheel
        log.info('Initializing filter wheel...')
        self.set_filter(self._filters[-1])
        self.set_filter('clear')

        # finished, send event
        self._change_motion_status(IMotion.Status.IDLE)
        self.comm.send_event(InitializedEvent())

    @timeout(300000)
    def park(self, *args, **kwargs):
        """Park telescope.

        Raises:
            ValueError: If telescope could not be parked.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # reset all offsets
        self._reset_offsets()

        # park telescope
        log.info('Parking telescope...')
        self._change_motion_status(IMotion.Status.PARKING)
        if not self._pilar.park():
            self._change_motion_status(IMotion.Status.ERROR)
            raise ValueError('Could not park telescope.')
        self._change_motion_status(IMotion.Status.PARKED)

    def get_temperatures(self, *args, **kwargs) -> dict:
        """Returns all temperatures measured by this module.

        Returns:
            Dict containing temperatures.
        """

        # lock status
        with self._lock:
            # get all temperatures
            temps = {}
            for name, var in self._temperatures.items():
                temps[name] = self._status[var]

            # return it
            return temps

    def stop_motion(self, device: str = None, *args, **kwargs):
        """Stop the motion.

        Args:
            device: Name of device to stop, or None for all.
        """
        self._pilar.stop()
        self._change_motion_status(IMotion.Status.IDLE)
        log.info('Stopped all motion.')

    def is_ready(self, *args, **kwargs) -> bool:
        """Returns the device is "ready", whatever that means for the specific device.

        Returns:
            True, if telescope is initialized and not in an error state.
        """

        # check that motion is not in one of the states listed below
        return self.get_motion_status() not in [IMotion.Status.PARKED, IMotion.Status.INITIALIZING,
                                                IMotion.Status.PARKING, IMotion.Status.ERROR, IMotion.Status.UNKNOWN]


__all__ = ['PilarTelescope']
