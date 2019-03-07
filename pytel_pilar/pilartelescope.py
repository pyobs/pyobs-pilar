import logging
import threading
import time
from threading import Lock

from astropy import units as u
from astropy.coordinates import SkyCoord

from pytel.interfaces import IFilters, IFitsHeaderProvider, IFocuser, IFocusModel
from pytel.modules import timeout
from pytel.modules.telescope.basetelescope import BaseTelescope
from pytel.utils.threads import LockWithAbort
from .pilardriver import PilarDriver

log = logging.getLogger(__name__)


class PilarTelescope(BaseTelescope, IFilters, IFitsHeaderProvider, IFocuser, IFocusModel):
    def __init__(self, *args, **kwargs):
        BaseTelescope.__init__(self, thread_funcs=[self._pilar_update, self._focus_tracker], *args, **kwargs)

        # init pilar
        log.info('Connecting to Pilar at {0:s}:{1:d}...'.format(self.config['pilar']['host'],
                                                                    self.config['pilar']['port']))
        self._pilar = PilarDriver(self.config['pilar']['host'], self.config['pilar']['port'],
                                  self.config['pilar']['username'], self.config['pilar']['password'])
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
        for var in self.config['pilar']['fits_headers'].keys():
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

    @classmethod
    def default_config(cls):
        cfg = super(PilarTelescope, cls).default_config()
        cfg['pilar'] = {
            'host': None, 'port': None, 'username': None, 'password': None,
            'fits_headers': None
        }
        return cfg

    def open(self) -> bool:
        if not BaseTelescope.open(self):
            return False

        # set shared variables
        self.comm.variables['InitTelescope'] = False

        # success
        return True

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

                # change variables
                self.comm.variables['InitTelescope'] = int(s['TELESCOPE.READY_STATE']) == 1

            except ValueError:
                # ignore it
                pass

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

    def status(self, *args, **kwargs) -> dict:
        # get parent
        s = super().status(*args, **kwargs)

        # create dict and add alt
        with self._lock:
            # telescope status
            status = BaseTelescope.Status.IDLE
            if float(self._status['TELESCOPE.READY_STATE']) == 0.:
                status = BaseTelescope.Status.PARKED
            elif 0. < float(self._status['TELESCOPE.READY_STATE']) < 1.:
                status = BaseTelescope.Status.INITPARK
            elif float(self._status['TELESCOPE.READY_STATE']) < 0.:
                status = BaseTelescope.Status.ERROR
            else:
                # telescope is initialized, check motion state
                ms = int(self._status['TELESCOPE.MOTION_STATE'])
                if ms & (1 << 0):
                    # first bit indicates moving
                    status = BaseTelescope.Status.SLEWING
                elif ms & (1 << 2):
                    # third bit indicates tracking
                    status = BaseTelescope.Status.TRACKING
                else:
                    # otherwise we're idle
                    status = BaseTelescope.Status.IDLE

            # telescope
            s['ITelescope'] = {
                'Status': status.value,
                'Position': {
                    'RA': self._status['POSITION.EQUATORIAL.RA_J2000'] * 15.,
                    'Dec': self._status['POSITION.EQUATORIAL.DEC_J2000'],
                    'Alt': self._status['POSITION.HORIZONTAL.ALT'],
                    'Az': self._status['POSITION.HORIZONTAL.AZ']
                }
            }

            # focus
            s['IFocuser'] = {
                'Focus': self._status['POSITION.INSTRUMENTAL.FOCUS.REALPOS']
            }

            # filter
            s['IFilter'] = {
                'Filter': self.get_filter()
            }

        # finished
        return s

    def get_fits_headers(self, *args, **kwargs) -> dict:
        # define values to request
        keys = {
            'CRVAL1': ('POSITION.EQUATORIAL.RA_J2000', 'Right ascension of telescope [degrees]'),
            'CRVAL2': ('POSITION.EQUATORIAL.DEC_J2000', 'Declination of telescope [degrees]'),
            'TEL-RA': ('POSITION.EQUATORIAL.RA_J2000', 'Right ascension of telescope [degrees]'),
            'TEL-DEC': ('POSITION.EQUATORIAL.DEC_J2000', 'Declination of telescope [degrees]'),
            'TEL-ZD': ('POSITION.HORIZONTAL.ZD', 'Telescope zenith distance [degrees]'),
            'TEL-ALT': ('POSITION.HORIZONTAL.ALT', 'Telescope altitude [degrees]'),
            'TEL-AZ': ('POSITION.HORIZONTAL.AZ', 'Telescope azimuth [degrees]'),
            'TEL-FOCU': ('POSITION.INSTRUMENTAL.FOCUS.REALPOS', 'Focus position [mm]'),
            'TEL-ROT': ('POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS', 'Derotator instrumental position at end [deg]'),
            'DEROTOFF': ('POINTING.SETUP.DEROTATOR.OFFSET', 'Derotator offset [deg]'),
            'AZOFF': ('POSITION.INSTRUMENTAL.AZ.OFFSET', 'Azimuth offset'),
            'ALTOFF': ('POSITION.INSTRUMENTAL.ZD.OFFSET', 'Altitude offset')
        }

        # add ones from config
        for var, h in self.config['pilar']['fits_headers'].items():
            keys[h[0]] = (var, h[1])

        # Monet/S: 3=T1, 1=T2
        # Monet/N: 2=T1, 1=T2

        # create dict and add alt and filter
        with self._lock:
            status = self._status.copy()
        hdr = {key: (status[var[0]], var[1]) for key, var in keys.items() if var[0] in status}

        # convert RA from hours to degrees
        hdr['CRVAL1'] = (hdr['CRVAL1'][0] * 15., hdr['CRVAL1'][1])
        hdr['TEL-RA'] = (hdr['TEL-RA'][0] * 15., hdr['TEL-RA'][1])

        # negative ALTOFF
        hdr['ALTOFF'] = (-hdr['ALTOFF'][0], hdr['ALTOFF'][1])

        # filter
        if 'POSITION.INSTRUMENTAL.FILTER[2].CURRPOS' in status:
            filter_id = status['POSITION.INSTRUMENTAL.FILTER[2].CURRPOS']
            hdr['FILTER'] = (self._pilar.filter_name(filter_id), 'Current filter')

        # sexagesimal
        if 'TEL-RA' in hdr and 'TEL-DEC' in hdr:
            # create sky coordinates
            c = SkyCoord(ra=hdr['TEL-RA'][0] * u.deg, dec=hdr['TEL-DEC'][0] * u.deg, frame='icrs')

            # convert
            hdr['RA'] = (str(c.ra.to_string(sep=':', unit=u.hour, pad=True)), 'Right ascension of telescope')
            hdr['DEC'] = (str(c.dec.to_string(sep=':', unit=u.deg, pad=True)), 'Declination of telescope')

        # return it
        return hdr

    def list_filters(self, *args, **kwargs) -> list:
        return self._filters

    def get_filter(self, *args, **kwargs) -> str:
        return self._pilar.filter_name()

    @timeout(20000)
    def set_filter(self, filter_name: str, *args, **kwargs) -> bool:
        # acquire lock
        with LockWithAbort(self._lock_filter, self._abort_filter):
            log.info('Changing filter to %s...', filter_name)
            self._pilar.change_filter(filter_name, abort_event=self._abort_filter)
            log.info('Filter changed.')
            return True

    def _move(self, alt: float, az: float, abort_event: threading.Event) -> bool:
        # reset offsets
        self.reset_offset()

        # start tracking
        log.info('Starting tracking at Alt=%.2f, Az=%.5f', alt, az)
        self.telescope_status = BaseTelescope.Status.SLEWING
        success = self._pilar.goto(alt, az, abort_event=abort_event)

        # finished
        if success:
            self.telescope_status = BaseTelescope.Status.IDLE
            log.info('Reached destination.')
        else:
            self.telescope_status = BaseTelescope.Status.IDLE
            log.warning('Could not reach destination.')
        return success

    def _track(self, ra: float, dec: float, abort_event: threading.Event) -> bool:
        # reset offsets
        self.reset_offset()

        # start tracking
        log.info('Starting tracking at RA=%.5f, Dec=%.5f', ra, dec)
        self.telescope_status = BaseTelescope.Status.SLEWING
        success = self._pilar.track(ra, dec, abort_event=abort_event)

        # finished
        if success:
            self.telescope_status = BaseTelescope.Status.TRACKING
            log.info('Reached destination.')
        else:
            self.telescope_status = BaseTelescope.Status.IDLE
            log.warning('Could not reach destination.')
        return success

    def get_focus(self, *args, **kwargs) -> float:
        return float(self._pilar.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS'))

    @timeout(300000)
    def set_focus(self, focus: float, *args, **kwargs) -> bool:
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
    def set_optimal_focus(self, *args, **kwargs) -> bool:
        """sets optimal focus"""

        # acquire lock
        with LockWithAbort(self._lock_focus, self._abort_focus):
            # calculate model
            focus = self._calc_optimal_focus()
            log.info('Setting optimal focus of %.2fmm and activating focus tracking...', focus)

            # set absolute focus
            self._pilar.focus(focus, abort_event=self._abort_focus)

            # activate optimal focus
            self._last_focus_time = time.time()

            # success
            return True

    def offset(self, dalt: float, daz: float, *args, **kwargs) -> bool:
        """Move to a given absolute offset in Alt/Az.

        Args:
            dalt: Altitude offset in degrees.
            daz: Azimuth offset in degrees.
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
        return True

    def reset_offset(self, *args, **kwargs) -> bool:
        self._offset_az = 0.
        self._offset_zd = 0.
        self._pilar.set('POSITION.INSTRUMENTAL.AZ.OFFSET', self._offset_az)
        self._pilar.set('POSITION.INSTRUMENTAL.ZD.OFFSET', self._offset_zd)
        return True

    @timeout(300000)
    def init(self, *args, **kwargs) -> bool:
        return self._pilar.init()

    @timeout(300000)
    def park(self, *args, **kwargs) -> bool:
        # reset all offsets
        self.reset_offset()
        # park telescope
        return self._pilar.park()
