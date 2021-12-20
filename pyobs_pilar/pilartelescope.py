import asyncio
import datetime
import logging
import os.path
from typing import Tuple, List, Dict, Any, Optional

from pyobs.mixins import FitsNamespaceMixin
from pyobs.events import FilterChangedEvent, OffsetsAltAzEvent
from pyobs.interfaces import IFilters, IFocuser, ITemperatures, IOffsetsAltAz, IPointingSeries
from pyobs.modules import timeout
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus
from pyobs.utils.threads import LockWithAbort
from .pilardriver import PilarDriver

log = logging.getLogger(__name__)


# TODO: use asyncio in driver directly
class PilarTelescope(BaseTelescope, IOffsetsAltAz, IFilters, IFocuser, ITemperatures, IPointingSeries,
                     FitsNamespaceMixin):
    def __init__(self, host: str, port: int, username: str, password: str,
                 pilar_fits_headers: Optional[Dict[str, Any]] = None, temperatures: Optional[Dict[str, str]] = None,
                 force_filter_forward: bool = True, pointing_path: Optional[str] = None, **kwargs: Any):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=['ITelescope', 'IFilters', 'IFocuser'])

        # add thread func
        self.add_background_task(self._pilar_update)

        # init
        self._pilar_connect = host, port, username, password
        self._filters: List[str] = []
        self._force_filter_forward = force_filter_forward
        self._pilar_fits_headers = pilar_fits_headers if pilar_fits_headers else {}
        self._temperatures = temperatures if temperatures else {}
        self._temperatures = {}

        # pilar
        self._pilar_connect = host, port, username, password
        log.info('Connecting to Pilar at %s:%d...', host, port)
        self._pilar = self.add_child_object(PilarDriver, host=host, port=port, username=username, password=password)

        # create update thread
        self._status: Dict[str, Any] = {}

        # optimal focus
        self._last_focus_time = None

        # some multi-threading stuff
        self._lock_focus = asyncio.Lock()
        self._abort_focus = asyncio.Event()
        self._lock_filter = asyncio.Lock()
        self._abort_filter = asyncio.Event()

        # pointing
        self._pointing_path = pointing_path
        self._pointing_id = 1

        # set Pilar variables for status updates...
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
        for var in self._pilar_fits_headers.keys():
            if var not in self._pilar_variables:
                self._pilar_variables.append(var)

        # ... and temperatures
        for var in self._temperatures.values():
            if var not in self._pilar_variables:
                self._pilar_variables.append(var)

        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    async def open(self) -> None:
        """Open module."""
        await BaseTelescope.open(self)

        # get list of filters
        self._filters = await self._pilar.filters()

        # subscribe to events
        if self.comm:
            await self.comm.register_event(FilterChangedEvent)

    async def close(self) -> None:
        await BaseTelescope.close(self)

        if self._pilar is not None:
            log.info('Closing connection to Pilar...')
            await self._pilar.close()
        log.info('Shutting down...')

    async def _pilar_update(self) -> None:
        # log
        log.info('Starting Pilar update thread...')

        while not self.closing.is_set():
            # no pilar connection yet?
            if self._pilar is None or not self._pilar.is_open:
                await asyncio.sleep(1)
                continue

            # catch everything
            try:
                # do nothing on error
                if self._pilar.has_error:
                    await asyncio.sleep(10)
                    continue

                # define values to request
                keys = self._pilar_variables

                # get data
                try:
                    multi = await self._pilar.get_multi(keys)
                except TimeoutError:
                    # sleep a little and continue
                    log.error('Request to Pilar timed out.')
                    await asyncio.sleep(60)
                    continue

                # check for ready state
                if 'TELESCOPE.READY_STATE' not in multi:
                    await asyncio.sleep(1)
                    continue

                # set status
                self._status = {}
                for key in keys:
                    try:
                        self._status[key] = float(multi[key])
                    except ValueError:
                        log.warning(f'Could not find {key} in response from Pilas.')

                # set motion status
                # we always set PARKED, INITIALIZING, ERROR, the others only on init
                if float(self._status['TELESCOPE.READY_STATE']) == 0.:
                    await self._change_motion_status(MotionStatus.PARKED)
                elif 0. < float(self._status['TELESCOPE.READY_STATE']) < 1.:
                    await self._change_motion_status(MotionStatus.INITIALIZING)
                elif float(self._status['TELESCOPE.READY_STATE']) < 0.:
                    await self._change_motion_status(MotionStatus.ERROR)
                else:
                    # telescope is initialized, check motion state
                    ms = int(self._status['TELESCOPE.MOTION_STATE'])
                    if ms & (1 << 0):
                        # first bit indicates moving
                        if await self.get_motion_status() == MotionStatus.UNKNOWN:
                            await self._change_motion_status(MotionStatus.SLEWING)
                    elif ms & (1 << 2):
                        # third bit indicates tracking
                        if await self.get_motion_status() == MotionStatus.UNKNOWN:
                            await self._change_motion_status(MotionStatus.TRACKING)
                    else:
                        # otherwise we're idle
                        if await self.get_motion_status() == MotionStatus.UNKNOWN:
                            await self._change_motion_status(MotionStatus.IDLE)

                # sleep a second
                await asyncio.sleep(1)

            except:
                log.exception('An unexpected error occured.')
                await asyncio.sleep(10)

        # log
        log.info('Shutting down Pilar update thread...')

    async def get_fits_header_before(self, namespaces: Optional[List[str]] = None, **kwargs: Any) \
            -> Dict[str, Tuple[Any, str]]:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # get headers from base
        hdr = await BaseTelescope.get_fits_header_before(self)

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
        status = self._status.copy()
        for key, v in keys.items():
            if v[0] in status:
                hdr[key] = (status[v[0]], v[1])

        # negative ALTOFF
        hdr['ALTOFF'] = (-hdr['ALTOFF'][0], hdr['ALTOFF'][1])

        # filter
        if 'POSITION.INSTRUMENTAL.FILTER[2].CURRPOS' in status:
            filter_id = status['POSITION.INSTRUMENTAL.FILTER[2].CURRPOS']
            hdr['FILTER'] = (self._pilar.filter_name(filter_id), 'Current filter')

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)

    async def get_radec(self, **kwargs: Any) -> Tuple[float, float]:
        """Returns current RA and Dec.

        Returns:
            Tuple of current RA and Dec in degrees.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError()

        # get RA/Dec
        return self._status['POSITION.EQUATORIAL.RA_J2000'] * 15., self._status['POSITION.EQUATORIAL.DEC_J2000']

    async def get_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        """Returns current Alt and Az.

        Returns:
            Tuple of current Alt and Az in degrees.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError

        # get Alt/Az
        return self._status['POSITION.HORIZONTAL.ALT'], self._status['POSITION.HORIZONTAL.AZ']

    async def list_filters(self, **kwargs: Any) -> List[str]:
        """List available filters.

        Returns:
            List of available filters.
        """
        return self._filters

    async def get_filter(self, **kwargs: Any) -> str:
        """Get currently set filter.

        Returns:
            Name of currently set filter.
        """
        return await self._pilar.filter_name()

    @timeout(60000)
    async def set_filter(self, filter_name: str, **kwargs: Any) -> None:
        """Set the current filter.

        Args:
            filter_name: Name of filter to set.

        Raises:
            ValueError: If binning could not be set.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # acquire lock
        async with LockWithAbort(self._lock_filter, self._abort_filter):
            log.info('Changing filter to %s...', filter_name)
            await self._change_motion_status(MotionStatus.SLEWING, interface='IFilters')
            await self._pilar.change_filter(filter_name, force_forward=self._force_filter_forward,
                                            abort_event=self._abort_filter)
            await self._change_motion_status(MotionStatus.POSITIONED, interface='IFilters')
            log.info('Filter changed.')

            # send event
            await self.comm.send_event(FilterChangedEvent(current=filter_name))

    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
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
        await self._reset_offsets()

        # start tracking
        await self._change_motion_status(MotionStatus.SLEWING, interface='ITelescope')
        success = self._pilar.goto(alt, az, abort_event=abort_event)
        await self._change_motion_status(MotionStatus.POSITIONED, interface='ITelescope')

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
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
        await self._reset_offsets()

        # start tracking
        await self._change_motion_status(MotionStatus.SLEWING, interface='ITelescope')
        success = self._pilar.track(ra, dec, abort_event=abort_event)
        await self._change_motion_status(MotionStatus.TRACKING, interface='ITelescope')

        # finished
        if success:
            log.info('Reached destination.')
        else:
            raise ValueError('Could not reach destination.')

    async def _reset_offsets(self) -> None:
        """Reset Alt/Az offsets."""
        await self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', 0)
        await self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', 0)

    async def get_focus(self, **kwargs: Any) -> float:
        """Return current focus.

        Returns:
            Current focus.
        """
        return float(await self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.CURRPOS'))

    async def get_focus_offset(self, **kwargs: Any) -> float:
        """Return current focus offset.

        Returns:
            Current focus offset.
        """
        return float(await self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET'))

    @timeout(30000)
    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        """Sets new focus.

        Args:
            focus: New focus value.

        Raises:
            InterruptedError: If focus was interrupted.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # acquire lock
        async with LockWithAbort(self._lock_focus, self._abort_focus):
            # start
            log.info('Setting focus to %.4f...', focus)
            await self._change_motion_status(MotionStatus.SLEWING, interface='IFocuser')
            #self._pilar.set('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS', focus,
            #                timeout=30000, abort_event=self._abort_focus)

            # set focus
            await self._pilar.focus(focus)

            # finished
            await self._change_motion_status(MotionStatus.POSITIONED, interface='IFocuser')
            log.info('Reached new focus of %.4f.',
                     float(await self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.CURRPOS')))

    @timeout(30000)
    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
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
        async with LockWithAbort(self._lock_focus, self._abort_focus):
            # set focus
            log.info('Setting focus offset to %.2f...', offset)
            await self._change_motion_status(MotionStatus.SLEWING, interface='IFocuser')
            await self._pilar.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', offset,
                                  timeout=10000)
            await self._change_motion_status(MotionStatus.POSITIONED, interface='IFocuser')
            log.info('Reached new focus offset of %.2f.',
                     float(await self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET')))

    @timeout(10000)
    async def set_offsets_altaz(self, dalt: float, daz: float, **kwargs: Any) -> None:
        """Move an Alt/Az offset.

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
        await self.comm.send_event(OffsetsAltAzEvent(alt=dalt, az=daz))
        old_status = await self.get_motion_status(interface='ITelescope')
        await self._change_motion_status(MotionStatus.SLEWING, interface='ITelescope')
        await self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', -dalt)
        await self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', daz)

        # just wait a second and finish
        await asyncio.sleep(5)
        await self._change_motion_status(old_status, interface='ITelescope')

    async def get_offsets_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        """Get Alt/Az offset.

        Returns:
            Tuple with alt and az offsets.
        """

        # get current offsets and return then
        dalt = -float(await self._pilar.get('POSITION.INSTRUMENTAL.ZD.OFFSET'))
        daz = float(await self._pilar.get('POSITION.INSTRUMENTAL.AZ.OFFSET'))
        return dalt, daz

    @timeout(300000)
    async def init(self, **kwargs: Any) -> None:
        """Initialize device.

        Raises:
            ValueError: If device could not be initialized.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # init telescope
        log.info('Initializing telescope...')
        await self._change_motion_status(MotionStatus.INITIALIZING)
        if not self._pilar.init():
            await self._change_motion_status(MotionStatus.ERROR)
            raise ValueError('Could not initialize telescope.')

        # init filter wheel
        log.info('Initializing filter wheel...')
        await self.set_filter(self._filters[-1])
        await self.set_filter('clear')

        # finished, send event
        await self._change_motion_status(MotionStatus.IDLE)

    @timeout(300000)
    async def park(self, **kwargs: Any) -> None:
        """Park device.

        Raises:
            ValueError: If device could not be parked.
        """

        # check error
        if self._pilar.has_error:
            raise ValueError('Telescope in error state.')

        # reset all offsets
        await self._reset_offsets()

        # park telescope
        log.info('Parking telescope...')
        await self._change_motion_status(MotionStatus.PARKING)
        if not self._pilar.park():
            await self._change_motion_status(MotionStatus.ERROR)
            raise ValueError('Could not park telescope.')
        await self._change_motion_status(MotionStatus.PARKED)

    async def get_temperatures(self, **kwargs: Any) -> Dict[str, float]:
        """Returns all temperatures measured by this module.

        Returns:
            Dict containing temperatures.
        """

        # get all temperatures
        temps = {}
        for name, var in self._temperatures.items():
            temps[name] = self._status[var]

        # return it
        return temps

    async def stop_motion(self, device: Optional[str] = None, **kwargs: Any) -> None:
        """Stop the motion.

        Args:
            device: Name of device to stop, or None for all.
        """
        await self._pilar.stop()
        await self._change_motion_status(MotionStatus.IDLE)
        log.info('Stopped all motion.')

    async def is_ready(self, **kwargs: Any) -> bool:
        """Returns the device is "ready", whatever that means for the specific device.

        Returns:
            Whether device is ready
        """

        # check that motion is not in one of the states listed below
        return await self.get_motion_status() not in [MotionStatus.PARKED, MotionStatus.INITIALIZING,
                                                      MotionStatus.PARKING, MotionStatus.ERROR, MotionStatus.UNKNOWN]

    async def start_pointing_series(self, **kwargs: Any) -> str:
        """Start a new pointing series.

        Returns:
            A unique ID or filename, by which the series can be identified.
        """

        # no path given?
        if self._pointing_path is None:
            raise ValueError('No path for pointing given in config.')
        log.info('Starting new pointing series...')

        # clear list of measurements
        await self._pilar.safe_set('POINTING.MODEL.CLEAR', 1, msg='Could not clear list of measurements: ')

        # set filename
        dt = datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        filename = os.path.join(self._pointing_path, f'pointing_{dt}.dat')
        await self._pilar.safe_set('POINTING.MODEL.FILE', filename, msg='Could not set filename: ')

        # check it
        log.info(f'Checking filename: {self._pilar.get("POINTING.MODEL.FILE")}.')

        # no auto-save
        await self._pilar.safe_set('POINTING.MODEL.AUTO_SAVE', 0, msg='Could not disable auto-saving: ')

        # finished
        return filename

    async def stop_pointing_series(self, **kwargs: Any) -> None:
        """Stop a pointing series."""

        # save model
        log.info('Stopping pointing series...')
        await self._pilar.safe_set('POINTING.MODEL.SAVE', 1)

    async def add_pointing_measure(self, **kwargs: Any) -> None:
        """Add a new measurement to the pointing series."""

        # add point
        log.info('Adding point to pointing series...')
        await self._pilar.safe_set('POINTING.MODEL.ADD', str(self._pointing_id), msg='Could not add measurement: ')
        self._pointing_id += 1

        # get number of points
        log.info(f'Number of measurements: {self._pilar.get("POINTING.MODEL.COUNT")}.')


__all__ = ['PilarTelescope']
