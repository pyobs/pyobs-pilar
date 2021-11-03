import asyncio
import logging
import threading
import time

from .pilarerror import PilarError


log = logging.getLogger(__name__)


class PilarCommand(object):
    def __init__(self, command):
        self.command = command
        self.id = None
        self.time = None
        self.sent = False
        self.acknowledged = False
        self.completed = False
        self.error = None
        self.data = None
        self.values = {}

    def __call__(self, transport):
        # send command
        cmd = str(self.id) + ' ' + self.command + '\n'
        transport.write(bytes(cmd, 'utf-8'))

        # store current time and set as sent
        self.time = time.time()
        self.sent = True

    def parse(self, line):
        # split line and check ID
        s = line.split()
        if int(s[0]) != self.id:
            return

        # acknowledge
        if 'COMMAND OK' in line or 'COMMAND ERROR' in line:
            self.acknowledged = True
            if 'COMMAND ERROR' in line:
                self.error = line

        # payload
        if 'DATA INLINE' in line:
            # get key and value
            pos = line.find('=')
            key = line[line.find('DATA INLINE') + 12:pos]
            value = line[pos+1:]
            # type?
            if value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            # store it
            self.values[key] = value
            # we always store the last result as data, makes it easier for commands requesting only a single value
            self.data = value

        # finish
        elif 'COMMAND COMPLETE' in line or 'COMMAND FAILED' in line:
            self.completed = True

    def wait(self, timeout: int = 5, abort_event: threading.Event = None):
        """Wait for the command to finish.

        Args:
            timeout: Timeout for waiting in seconds.
            abort_event: When set, wait is aborted.
        """

        # wait for data
        while not self.completed:
            # sleep a little
            if abort_event is not None:
                abort_event.wait(0.1)

                # abort event set?
                if abort_event.is_set():
                    return

            else:
                time.sleep(0.1)

            # timeout reached?
            if time.time() - self.time > timeout:
                raise TimeoutError('Command took too long to execute.')


class PilarClientProtocol(asyncio.Protocol):
    """ asyncio.Protocol implementation for the Pilar interface. """

    def __init__(self, driver, loop, username, password):
        """ Creates a SicamTcpClientProtocol.
        :param driver: SicamTcpDriver instance.
        :param loop: asyncio event loop.
        :param username: Username for login.
        :param password: Password for login
        :return:
        """

        # init some stuff
        self._driver = driver
        self._buffer = ''
        self._loop = loop
        self._transport = None
        self._username = username
        self._password = password
        self._logged_in = False
        self._id = 0
        self._commands = []

        # store self in driver
        self._driver.protocol = self

    @property
    def logged_in(self):
        return self._logged_in

    def connection_made(self, transport):
        """ Called, when the protocol is connected to the server.
        :param transport: Transport connected to server.
        :return:
        """

        # store transport
        self._transport = transport

    @asyncio.coroutine
    def stop(self):
        """ Disconnect gracefully. """

        # send disconnect
        log.info('Sending disconnect...')
        self._transport.write(b'disconnect')

        # disconnect
        self._transport.close()
        self._loop.stop()
        log.info('Disconnected from pilar.')

    def data_received(self, data):
        """ Called, when new data arrives from the server.
        :param data: New chunk of data.
        :return:
        """

        # add data to buffer
        self._buffer += data.decode('utf-8')

        # create as many packets as possible
        while self._buffer and '\n' in self._buffer:
            # extract line from buffer
            length = self._buffer.find('\n')
            line = self._buffer[:length]
            self._buffer = self._buffer[length+1:]

            # AUTH?
            if 'AUTH PLAIN' in line:
                log.info('Logging into Pilar...')
                # send AUTH line
                auth = 'AUTH PLAIN "' + self._username + '" "' \
                       + self._password + '"\n'
                self._transport.write(bytes(auth, 'utf-8'))

            elif 'AUTH OK' in line:
                log.info('Authentication for Pilar successful.')
                self._logged_in = True

            elif 'AUTH FAILED' in line:
                log.warning('Authentication for Pilar failed.')
                self._logged_in = False

            else:
                # loop all commands and parse line
                commands_to_delete = []
                for cmd in self._commands:
                    # parse line
                    cmd.parse(line)

                    # is command finished?
                    if cmd.completed:
                        # remove it from list
                        commands_to_delete.append(cmd)

                # delete finished commands
                for cmd in commands_to_delete:
                    self._commands.remove(cmd)

    def connection_lost(self, exc):
        """ Called, when connection to server is lost. """
        self._loop.stop()

    def execute(self, command):
        # get next id
        self._id += 1

        # create command
        cmd = PilarCommand(command)
        cmd.id = self._id
        self._commands.append(cmd)

        # execute return
        cmd(self._transport)
        return cmd


class PilarDriver(object):
    """ Wrapper for easy communication with Pilar. """

    def __init__(self, host, port, username, password):
        """ Create new driver. """

        # init some stuff
        self._host = host
        self._port = port
        self._username = username
        self._password = password

        self._loop = None
        self._thread = None
        self._filters = []
        self.protocol = None
        self._closing_event = threading.Event()

        # errors
        self._has_error = False
        self._error_thread = None

    def open(self) -> bool:
        """ Open connection to SIImage. """

        # create and start thread
        self._thread = threading.Thread(target=self._thread_function, name='pilar')
        self._thread.start()

        # errors
        self._error_thread = threading.Thread(target=self._error_thread_func, name='pilarerr')
        self._error_thread.start()

        # wait for connection
        while self.protocol is None:
            time.sleep(0.1)

        # success
        return True

    def wait_for_login(self):
        while not self.protocol.logged_in:
            time.sleep(0.1)

    def _thread_function(self):
        """ Thread for asyncio event loop. """

        # create event loop
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        # create connection
        coro = self._loop.create_connection\
            (lambda: PilarClientProtocol(self, self._loop,
                                         self._username, self._password),
             self._host, self._port)

        # run event loop, until connection is made
        self._loop.run_until_complete(coro)

        # run loop forever (or at least until loop is stopped)
        self._loop.run_forever()

        # close loop
        self._loop.close()

    def _error_thread_func(self):
        # run until closing
        while not self._closing_event.is_set():
            # not logged in?
            if self.protocol is None or not self.protocol.logged_in:
                self._closing_event.wait(5)

            # check for errors and clear them
            self._has_error = not self.clear_errors()

            # on error, wait and check
            if self._has_error:
                # wait a little
                self._closing_event.wait(5)

                # check again
                self._has_error = not self.check_errors()

            # wait five seconds
            self._closing_event.wait(5)

    @property
    def has_error(self):
        return self._has_error

    def close(self):
        """ Close connection to SIImage. """

        # safely close the connection
        self._closing_event.set()
        asyncio.run_coroutine_threadsafe(self.protocol.stop(), loop=self._loop)

    @property
    def is_open(self):
        """ Whether connection is open."""
        return self.protocol is not None

    def get(self, key):
        cmd = self.protocol.execute('GET ' + key)
        cmd.wait()
        return cmd.data

    def get_multi(self, keys):
        # join keys with ";" and execute
        cmd = self.protocol.execute('GET ' + ';'.join(keys))
        cmd.wait()
        return cmd.values

    def set(self, key, value, wait: bool = True, timeout: int = 5000, abort_event: threading.Event = None):
        """Set a variable with a given value.

        Args:
            key: Name of variable to set.
            value: New value.
            wait: Whether or not to wait for command.
            timeout: Timeout for waiting.
            abort_event: When set, wait is aborted.
        """

        # execute SET command
        cmd = self.protocol.execute(f'SET {key}="{str(value)}"')

        # want to wait?
        if wait:
            cmd.wait(timeout=timeout, abort_event=abort_event)
            return cmd.error is None

        # return cmd
        return cmd

    def safe_set(self, key, value, timeout: int = 5000, abort_event: threading.Event = None, msg: str = ''):
        """Set a variable with a given value, raise exception on error.

        Args:
            key: Name of variable to set.
            value: New value.
            timeout: Timeout for waiting.
            abort_event: When set, wait is aborted.
            msg: Message to add to exception text.
        """

        # execute SET command
        cmd = self.protocol.execute(f'SET {key}="{str(value)}"')

        # wait
        cmd.wait(timeout=timeout, abort_event=abort_event)
        if cmd.error is not None:
            raise ValueError(msg + cmd.error)

    def list_errors(self):
        """Fetch list of errors from telescope.

        From OpenTSI documentation about TELESCOPE.STATUS.LIST:
            A comma separated list of function groups that currently have
            problems in the following format:
            <group>|<level>[:<component>|<level>[;<component>...]]
            [:<error>|<detail>|<level>|<component>[;<error>...]]
            [,<group>...]
            <group> One of the above listed function groups
            <level> Bitwise “OR” of all errors in the group resp. compo-
            nent or for the individual error. The bits have the same
            meaning as for GLOBAL.
            <component> The OpenTCI module name (possibly includ-
            ing a index in []).
            <error> The hardware specific error code
            <detail> The hardware specific detail information for the
            error code.
            The information from <error>/<detail> should only be used
            for logging, as it is hardware specific and may change at any
            time.
            At most one entry per group is generated. If the delimiters
            should occur within the names or messages, they will be either
            escaped with a backslash or the entire entry is put in double
            quotes.
        """

        # init error list
        error_list = []

        # get list of errors
        errors = self.get('TELESCOPE.STATUS.LIST')

        # divide into groups and loop them
        for group in errors.split(','):
            # find last colon and split everything after that at semicolon
            errors = group[group.rfind(':')+1:].split(';')

            # loop errors
            for error in errors:
                # split by | to get name of error
                name = error.split('|')[0]
                if len(name) == 0:
                    continue

                # create error and add to list
                err = PilarError.create(name)
                error_list.append(err)

        # return list of errors
        return error_list

    def clear_errors(self):
        """Clears Pilar errors."""

        # get telescope status
        level = int(self.get('TELESCOPE.STATUS.GLOBAL'))

        # check level
        if level & (0x01 | 0x02):
            log.warning('Found severe errors with level %d.', level)
        else:
            return True
            #log.info('Current error level is %d.', level)

        # check fatal errors
        self.list_errors()
        if PilarError.check_fatal():
            log.error('Cannot clear errors, fatal condition.')
            return False

        # do clearing
        log.info('Clearing telescope errors...')
        self.set('TELESCOPE.STATUS.CLEAR', level)

    def check_errors(self):
        """Check for errors after clearing."""

        # get telescope status
        level = int(self.get('TELESCOPE.STATUS.GLOBAL'))

        # check level
        if level & (0x01 | 0x02):
            log.error('Could not clear severe errors.')
            return False
        else:
            log.info('Remaining error level is %d.', level)
            return True

    def init(self, attempts: int = 3, wait: float = 10., attempt_timeout: float = 30.):
        """Initialize telescope

        Args:
            attempts (int): Number of attempts for initializing telescope.
            wait (float):   Wait time in seconds after sending command.
            attempt_timeout (float): Number of seconds to allow for each attempt.

        Returns:

        """

        # check, whether telescope is initialized already
        ready = self.get('TELESCOPE.READY_STATE')
        if float(ready) == 1.:
            log.info("Telescope already initialized.")
            return True

        # we give it a couple of attempts
        log.info("Initializing telescope...")
        for attempt in range(attempts):
            # init telescope
            self.set('TELESCOPE.READY', 1)

            # sleep a little
            time.sleep(wait)

            # wait for init
            waited = 0.
            while waited < attempt_timeout:
                # get status
                ready = self.get('TELESCOPE.READY_STATE')
                if float(ready) == 1.:
                    log.info("Telescope initialized.")
                    return True

                # sleep  a little
                waited += 0.5
                time.sleep(0.5)

        # we should never arrive here
        log.error('Could not initialize telescope.')
        return False

    def park(self, attempts: int = 3, wait: float = 10., attempt_timeout: float = 30.):
        # check, whether telescope is parked already
        ready = self.get('TELESCOPE.READY_STATE')
        if float(ready) == 0.:
            log.info("Telescope already parked.")
            return True

        # we give it a couple of attempts
        log.info("Parking telescope...")
        for attempt in range(attempts):
            # parking telescope
            self.set('TELESCOPE.READY', 0)

            # sleep a little
            time.sleep(wait)

            # wait for init
            waited = 0.
            while waited < attempt_timeout:
                # get status
                ready = self.get('TELESCOPE.READY_STATE')
                if float(ready) == 0.:
                    log.info("Telescope parked.")
                    return True

        # we should never arrive here
        log.error('Could not park telescope.')
        return False

    def reset_focus_offset(self):
        # get focus and offset
        focus = float(self.get('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS'))
        offset = float(self.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET'))

        # need to do something?
        if abs(offset) > 1e-5:
            # set new
            cmd1 = self.set('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS', focus + offset, wait=False)
            cmd2 = self.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', 0, wait=False)

            # wait for both
            self.wait_for_all([cmd1, cmd2])

    @staticmethod
    def wait_for_all(commands):
        [cmd.wait() for cmd in commands]

    def focus(self, position, timeout=30000, accuracy=0.01, sleep=500, retry=3,
              sync_thermal=False, sync_port=False, sync_filter=False, disable_tracking=False,
              abort_event: threading.Event=None) -> bool:
        # reset any offset
        #self.reset_focus_offset()

        # set sync_mode, first bit is always set
        sync_mode = 1
        if sync_thermal:
            log.info('Enabling synchronization with thermal model...')
            sync_mode |= 1 << 1
        if sync_port:
            log.info('Enabling synchronization with port specific offset...')
            sync_mode |= 1 << 2
        if sync_filter:
            log.info('Enabling synchronization with filter specific offset...')
            sync_mode |= 1 << 3
        if disable_tracking:
            log.info('Turning off focus motor during tracking...')
            sync_mode |= 1 << 4

        # setting new focus
        log.info('Setting new focus value to %.3fmm...', position)
        self.set('POINTING.SETUP.FOCUS.SYNCMODE', sync_mode)
        #self.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', 0)
        self.set('POINTING.SETUP.FOCUS.POSITION', position)
        self.set('POINTING.TRACK', 4)

        # loop until finished
        delta = 1e10
        waited = 0
        attempts = 0
        while delta >= accuracy:
            # abort?
            if abort_event is not None and abort_event.is_set():
                return False

            # sleep a little
            time.sleep(sleep / 1000.)
            waited += sleep

            # get focus distance
            delta = abs(float(self.get('POSITION.INSTRUMENTAL.FOCUS.TARGETDISTANCE')))
            log.info('Distance to new focus: %.3fmm.', delta)

            # waiting too long?
            if waited > timeout:
                # got more retries?
                if attempts < retry:
                    # yes, so try again
                    attempts += 1
                    waited = 0
                    log.warning('Focus timeout, starting attempt %d.', attempts+1)
                    self.set('POINTING.SETUP.FOCUS.POSITION', position)
                    self.set('POINTING.TRACK', 4)

                else:
                    # no, we're out of time
                    log.error('Focusing not possible.')
                    return False

        # get new focus
        foc = self.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS')
        log.info('New focus position reached: %.3fmm.', float(foc))
        return True

    def goto(self, alt, az, abort_event: threading.Event) -> bool:
        # stop telescope
        self.set('POINTING.TRACK', 0)

        # prepare derotator
        self.set('POINTING.SETUP.DEROTATOR.SYNCMODE', 3)
        self.set('POINTING.SETUP.DEROTATOR.OFFSET', 0.)

        # set new coordinates
        self.set('OBJECT.HORIZONTAL.AZ', az)
        self.set('OBJECT.HORIZONTAL.ALT', alt)

        # start moving
        self.set('POINTING.TRACK', 2)

        # wait for it
        for attempt in range(5):
            # abort?
            if abort_event.is_set():
                return False

            # wait
            success = self._wait_for_value('TELESCOPE.MOTION_STATE', '0', abort_event=abort_event)
            if success:
                break

            # sleep a little and try again
            abort_event.wait(1)
            self.set('POINTING.TRACK', 2)
            log.warning('Attempt %d for moving to position failed.', attempt + 1)

        # success
        return success

    def track(self, ra, dec, abort_event: threading.Event) -> bool:
        # stop telescope
        self.set('POINTING.TRACK', 0)

        # prepare derotator
        self.set('POINTING.SETUP.DEROTATOR.SYNCMODE', 3)
        self.set('POINTING.SETUP.DEROTATOR.OFFSET', 0.)

        # set new coordinates
        self.set('OBJECT.EQUATORIAL.RA', ra/15.)
        self.set('OBJECT.EQUATORIAL.DEC', dec)

        # start tracking
        self.set('POINTING.TRACK', 1)

        # wait for it
        for attempt in range(5):
            # abort?
            if abort_event.is_set():
                return False

            # wait
            success = self._wait_for_value('TELESCOPE.MOTION_STATE', '11', '0', abort_event=abort_event)
            if success:
                break

            # got any errors?
            if len(self.list_errors()) > 0:
                return False

            # sleep a little and try again
            abort_event.wait(1)
            self.set('POINTING.TRACK', 2)
            log.warning('Attempt %d for moving to position failed.', attempt + 1)

        # success
        return success

    def _wait_for_value(self, var, value, not_value=None, abort_event: threading.Event = None):
        # sleep a little
        time.sleep(0.5)

        while True:
            # abort?
            if abort_event.is_set():
                return False

            # get variable
            val = float(self.get(var))

            # check
            if val == float(value):
                return True
            elif not_value is not None and val == float(not_value):
                return False

            # sleep a little
            time.sleep(1)

    def fits_data(self):
        return {
            'TEL-T1': float(self.get('AUXILIARY.SENSOR[3].VALUE')),
            'TEL-T2': float(self.get('AUXILIARY.SENSOR[1].VALUE')),
            'TEL-T3': float(self.get('AUXILIARY.SENSOR[2].VALUE')),
            'TEL-T4': float(self.get('AUXILIARY.SENSOR[4].VALUE')),
            'TEL-FOCU': float(self.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS'))
        }

    def init_filters(self):
        # get number of filters
        num = int(self.get('TELESCOPE.CONFIG.PORT[2].FILTER'))
        log.info("Found %d filters.", num)

        # loop all filters
        self._filters = []
        for i in range(num):
            # set filter
            self.set('POINTING.SETUP.FILTER.INDEX', i)

            # get filter name
            name = self.get('POINTING.SETUP.FILTER.NAME')

            # strip quotes
            name = name.replace('"', '')

            # append to list
            log.info('Found filter %s.', name)
            self._filters.append(name)

    def filters(self):
        if not self._filters:
            self.init_filters()
        return self._filters

    def change_filter(self, filter_name, force_forward: bool = True, abort_event: threading.Event = None):
        # get current filter id
        cur_id = int(float(self.get('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS')))

        # find ID of filter
        filter_id = self._filters.index(filter_name)
        if filter_id == cur_id:
            return True
        log.info('Changing to filter %s with ID %d.', filter_name, filter_id)

        # force only forward motion? if new ID is smaller than current one, first move to last filter
        if force_forward:
            # do until we're at the current filter
            while cur_id != filter_id:
                # how far can we go?
                for i in range(3):
                    # increase cur filter by one, wrap at end
                    cur_id += 1
                    if cur_id >= len(self._filters):
                        cur_id = 0

                    # got it?
                    if cur_id == filter_id:
                        break

                # move there
                if not self._change_filter_to_id(cur_id, abort_event):
                    log.info('Could not change to filter.')
                    return False

            # finished
            log.info('Successfully changed to filter %s.', filter_name)
            return True

        else:
            # simply go to requested filter
            if self._change_filter_to_id(filter_id, abort_event):
                log.info('Successfully changed to filter %s.', self._filters[filter_id])
                return True
            else:
                log.info('Could not change filter.')
                return False

    def _change_filter_to_id(self, filter_id: int, abort_event: threading.Event = None):
        # set it
        self.set('POINTING.SETUP.FILTER.INDEX', filter_id)
        self.set('POINTING.TRACK', 3)

        # wait for it
        return self._wait_for_value('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS', filter_id, abort_event=abort_event)

    def filter_name(self, filter_id: int=None):
        if filter_id is None:
            filter_id = float(self.get('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS'))
        return self._filters[int(filter_id)]

    def stop(self):
        # stop telescope
        # TODO: there is obviously some kind of ABORT command, look into it

        # deactivate tracking
        self.set('POINTING.TRACK', 0)
