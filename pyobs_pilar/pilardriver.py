from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional, Dict, List, Union

from pyobs.object import Object
from .pilarerror import PilarError


log = logging.getLogger(__name__)


class PilarCommand(object):
    def __init__(self, command: str):
        self.command = command
        self.id: Optional[int] = None
        self.time: Optional[float] = None
        self.sent = False
        self.acknowledged = False
        self.completed = asyncio.Event()
        self.error: Optional[str] = None
        self.data = Optional[Any]
        self.values: Dict[str, Any] = {}

    def __call__(self, transport: asyncio.Transport) -> None:
        # send command
        cmd = str(self.id) + ' ' + self.command + '\n'
        transport.write(bytes(cmd, 'utf-8'))

        # store current time and set as sent
        self.time = time.time()
        self.sent = True

    def parse(self, line: str) -> None:
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
            self.completed.set()

    async def wait(self, timeout: int = 5) -> None:
        """Wait for the command to finish.

        Args:
            timeout: Timeout for waiting in seconds.
        """
        await asyncio.wait_for(self.completed.wait(), timeout)


class PilarClientProtocol(asyncio.Protocol):
    """ asyncio.Protocol implementation for the Pilar interface. """

    def __init__(self, driver: PilarDriver, loop: asyncio.AbstractEventLoop, username: str, password: str):
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
        self._transport: Optional[asyncio.Transport] = None
        self._username = username
        self._password = password
        self._logged_in = False
        self._id: int = 0
        self._commands: List[PilarCommand] = []

        # store self in driver
        self._driver.protocol = self

    @property
    def logged_in(self) -> float:
        return self._logged_in

    def connection_made(self, transport):
        """ Called, when the protocol is connected to the server.
        :param transport: Transport connected to server.
        :return:
        """

        # store transport
        self._transport = transport

    async def stop(self) -> None:
        """ Disconnect gracefully. """

        if self._transport:
            # send disconnect
            log.info('Sending disconnect...')
            self._transport.write(b'disconnect')

            # disconnect
            self._transport.close()
            log.info('Disconnected from pilar.')

    def data_received(self, data: bytes) -> None:
        """ Called, when new data arrives from the server.
        :param data: New chunk of data.
        :return:
        """

        # this cannot happen, but let's make MyPy happy
        if self._transport is None:
            return

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
                    if cmd.completed.is_set():
                        # remove it from list
                        commands_to_delete.append(cmd)

                # delete finished commands
                for cmd in commands_to_delete:
                    self._commands.remove(cmd)

    def execute(self, command: str) -> PilarCommand:
        if self._transport is None:
            raise RuntimeError()

        # get next id
        self._id += 1

        # create command
        cmd = PilarCommand(command)
        cmd.id = self._id
        self._commands.append(cmd)

        # execute return
        cmd(self._transport)
        return cmd


class PilarDriver(Object):
    """ Wrapper for easy communication with Pilar. """

    def __init__(self, host: str, port: int, username: str, password: str, **kwargs: Any):
        """ Create new driver. """
        Object.__init__(self, **kwargs)

        # init some stuff
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._filters: List[str] = []
        self.protocol: Optional[PilarClientProtocol] = None

        # errors
        self._has_error = False
        self._error_thread = None

        # background tasks
        self.add_background_task(self._error_background_task)

    async def open(self) -> None:
        """ Open connection to SIImage. """

        # create connection
        loop = asyncio.get_running_loop()
        await loop.create_connection(lambda: PilarClientProtocol(self, loop, self._username, self._password),
                                     self._host, self._port)

        # wait for login
        while self.protocol is None or not self.protocol.logged_in:
            await asyncio.sleep(0.1)

    async def close(self) -> None:
        """ Close connection to SIImage. """

        # safely close the connection
        if self.protocol:
            await self.protocol.stop()

    async def _error_background_task(self) -> None:
        # run until closing
        while True:
            # not logged in?
            if self.protocol is None or not self.protocol.logged_in:
                await asyncio.sleep(5)

            # check for errors and clear them
            self._has_error = not await self.clear_errors()

            # on error, wait and check
            if self._has_error:
                # wait a little
                await asyncio.sleep(5)

                # check again
                self._has_error = not await self.check_errors()

            # wait five seconds
            await asyncio.sleep(5)

    @property
    def has_error(self) -> bool:
        return self._has_error

    @property
    def is_open(self) -> bool:
        """ Whether connection is open."""
        return self.protocol is not None

    async def get(self, key: str) -> Any:
        if self.protocol is None:
            raise RuntimeError()
        cmd = self.protocol.execute('GET ' + key)
        await cmd.wait()
        return cmd.data

    async def get_multi(self, keys: List[str]) -> Dict[str, Any]:
        if self.protocol is None:
            raise RuntimeError()
        # join keys with ";" and execute
        cmd = self.protocol.execute('GET ' + ';'.join(keys))
        await cmd.wait()
        return cmd.values

    async def set(self, key: str, value: Any, wait: bool = True, timeout: int = 5000) -> Union[bool, PilarCommand]:
        """Set a variable with a given value.

        Args:
            key: Name of variable to set.
            value: New value.
            wait: Whether or not to wait for command.
            timeout: Timeout for waiting.
        """
        if self.protocol is None:
            raise RuntimeError()

        # execute SET command
        cmd = self.protocol.execute(f'SET {key}="{str(value)}"')

        # want to wait?
        if wait:
            await cmd.wait(timeout=timeout)
            return cmd.error is None

        # return cmd
        return cmd

    async def safe_set(self, key: str, value: Any, timeout: int = 5000, msg: str = '') -> None:
        """Set a variable with a given value, raise exception on error.

        Args:
            key: Name of variable to set.
            value: New value.
            timeout: Timeout for waiting.
            msg: Message to add to exception text.
        """
        if self.protocol is None:
            raise RuntimeError()

        # execute SET command
        cmd = self.protocol.execute(f'SET {key}="{str(value)}"')

        # wait
        await cmd.wait(timeout=timeout)
        if cmd.error is not None:
            raise ValueError(msg + cmd.error)

    async def list_errors(self) -> List[PilarError]:
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
        error_list: List[PilarError] = []

        # get list of errors
        errors = await self.get('TELESCOPE.STATUS.LIST')

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
                if err is None:
                    raise ValueError('Error while handling error.')
                else:
                    error_list.append(err)

        # return list of errors
        return error_list

    async def clear_errors(self) -> bool:
        """Clears Pilar errors."""

        # get telescope status
        level = int(await self.get('TELESCOPE.STATUS.GLOBAL'))

        # check level
        if level & (0x01 | 0x02):
            log.warning('Found severe errors with level %d.', level)
        else:
            return True
            #log.info('Current error level is %d.', level)

        # check fatal errors
        await self.list_errors()
        if PilarError.check_fatal():
            log.error('Cannot clear errors, fatal condition.')
            return False

        # do clearing
        log.info('Clearing telescope errors...')
        await self.set('TELESCOPE.STATUS.CLEAR', level)
        return True

    async def check_errors(self) -> bool:
        """Check for errors after clearing."""

        # get telescope status
        level = int(await self.get('TELESCOPE.STATUS.GLOBAL'))

        # check level
        if level & (0x01 | 0x02):
            log.error('Could not clear severe errors.')
            return False
        else:
            log.info('Remaining error level is %d.', level)
            return True

    async def init(self, attempts: int = 3, wait: float = 10., attempt_timeout: float = 30.) -> bool:
        """Initialize telescope

        Args:
            attempts (int): Number of attempts for initializing telescope.
            wait (float):   Wait time in seconds after sending command.
            attempt_timeout (float): Number of seconds to allow for each attempt.

        Returns:

        """

        # check, whether telescope is initialized already
        ready = await self.get('TELESCOPE.READY_STATE')
        if float(ready) == 1.:
            log.info("Telescope already initialized.")
            return True

        # we give it a couple of attempts
        log.info("Initializing telescope...")
        for attempt in range(attempts):
            # init telescope
            await self.set('TELESCOPE.READY', 1)

            # sleep a little
            await asyncio.sleep(wait)

            # wait for init
            waited = 0.
            while waited < attempt_timeout:
                # get status
                ready = await self.get('TELESCOPE.READY_STATE')
                if float(ready) == 1.:
                    log.info("Telescope initialized.")
                    return True

                # sleep  a little
                waited += 0.5
                await asyncio.sleep(0.5)

        # we should never arrive here
        log.error('Could not initialize telescope.')
        return False

    async def park(self, attempts: int = 3, wait: float = 10., attempt_timeout: float = 30.) -> bool:
        # check, whether telescope is parked already
        ready = await self.get('TELESCOPE.READY_STATE')
        if float(ready) == 0.:
            log.info("Telescope already parked.")
            return True

        # we give it a couple of attempts
        log.info("Parking telescope...")
        for attempt in range(attempts):
            # parking telescope
            await self.set('TELESCOPE.READY', 0)

            # sleep a little
            await asyncio.sleep(wait)

            # wait for init
            waited = 0.
            while waited < attempt_timeout:
                # get status
                ready = await self.get('TELESCOPE.READY_STATE')
                if float(ready) == 0.:
                    log.info("Telescope parked.")
                    return True

        # we should never arrive here
        log.error('Could not park telescope.')
        return False

    async def reset_focus_offset(self) -> None:
        # get focus and offset
        focus = float(await self.get('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS'))
        offset = float(await self.get('POSITION.INSTRUMENTAL.FOCUS.OFFSET'))

        # need to do something?
        if abs(offset) > 1e-5:
            # set new
            cmd1 = await self.set('POSITION.INSTRUMENTAL.FOCUS.TARGETPOS', focus + offset, wait=False)
            cmd2 = await self.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', 0, wait=False)

    async def focus(self, position: float, timeout: int = 30000, accuracy: float = 0.01, sleep: int = 500,
                    retry: int = 3, sync_thermal: bool = False, sync_port: bool = False, sync_filter: bool = False,
                    disable_tracking: bool = False, abort_event: Optional[asyncio.Event] = None) -> bool:
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
        await self.set('POINTING.SETUP.FOCUS.SYNCMODE', sync_mode)
        #self.set('POSITION.INSTRUMENTAL.FOCUS.OFFSET', 0)
        await self.set('POINTING.SETUP.FOCUS.POSITION', position)
        await self.set('POINTING.TRACK', 4)

        # loop until finished
        delta = 1e10
        waited = 0
        attempts = 0
        while delta >= accuracy:
            # abort?
            if abort_event is not None and abort_event.is_set():
                return False

            # sleep a little
            await asyncio.sleep(sleep / 1000.)
            waited += sleep

            # get focus distance
            delta = abs(float(await self.get('POSITION.INSTRUMENTAL.FOCUS.TARGETDISTANCE')))
            log.info('Distance to new focus: %.3fmm.', delta)

            # waiting too long?
            if waited > timeout:
                # got more retries?
                if attempts < retry:
                    # yes, so try again
                    attempts += 1
                    waited = 0
                    log.warning('Focus timeout, starting attempt %d.', attempts+1)
                    await self.set('POINTING.SETUP.FOCUS.POSITION', position)
                    await self.set('POINTING.TRACK', 4)

                else:
                    # no, we're out of time
                    log.error('Focusing not possible.')
                    return False

        # get new focus
        foc = await self.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS')
        log.info('New focus position reached: %.3fmm.', float(foc))
        return True

    async def goto(self, alt: float, az: float, abort_event: asyncio.Event) -> bool:
        # stop telescope
        await self.set('POINTING.TRACK', 0)

        # prepare derotator
        await self.set('POINTING.SETUP.DEROTATOR.SYNCMODE', 3)
        await self.set('POINTING.SETUP.DEROTATOR.OFFSET', 0.)

        # set new coordinates
        await self.set('OBJECT.HORIZONTAL.AZ', az)
        await self.set('OBJECT.HORIZONTAL.ALT', alt)

        # start moving
        await self.set('POINTING.TRACK', 2)

        # wait for it
        success = False
        for attempt in range(5):
            # abort?
            if abort_event.is_set():
                return False

            # wait
            success = await self._wait_for_value('TELESCOPE.MOTION_STATE', '0', abort_event=abort_event)
            if success:
                break

            # sleep a little and try again
            await asyncio.sleep(1)
            await self.set('POINTING.TRACK', 2)
            log.warning('Attempt %d for moving to position failed.', attempt + 1)

        # success
        return success

    async def track(self, ra: float, dec: float, abort_event: asyncio.Event) -> bool:
        # stop telescope
        await self.set('POINTING.TRACK', 0)

        # prepare derotator
        await self.set('POINTING.SETUP.DEROTATOR.SYNCMODE', 3)
        await self.set('POINTING.SETUP.DEROTATOR.OFFSET', 0.)

        # set new coordinates
        await self.set('OBJECT.EQUATORIAL.RA', ra/15.)
        await self.set('OBJECT.EQUATORIAL.DEC', dec)

        # start tracking
        await self.set('POINTING.TRACK', 1)

        # wait for it
        success = False
        for attempt in range(5):
            # abort?
            if abort_event.is_set():
                return False

            # wait
            success = await self._wait_for_value('TELESCOPE.MOTION_STATE', '11', '0', abort_event=abort_event)
            if success:
                break

            # got any errors?
            if len(await self.list_errors()) > 0:
                return False

            # sleep a little and try again
            await asyncio.sleep(1)
            await self.set('POINTING.TRACK', 2)
            log.warning('Attempt %d for moving to position failed.', attempt + 1)

        # success
        return success

    async def _wait_for_value(self, var: str, value: Any, not_value: Optional[Any] = None,
                              abort_event: Optional[asyncio.Event] = None) -> bool:
        # sleep a little
        await asyncio.sleep(0.5)

        while True:
            # abort?
            if abort_event is not None and abort_event.is_set():
                return False

            # get variable
            val = float(await self.get(var))

            # check
            if val == float(value):
                return True
            elif not_value is not None and val == float(not_value):
                return False

            # sleep a little
            await asyncio.sleep(1)

    async def fits_data(self) -> Dict[str, float]:
        return {
            'TEL-T1': float(await self.get('AUXILIARY.SENSOR[3].VALUE')),
            'TEL-T2': float(await self.get('AUXILIARY.SENSOR[1].VALUE')),
            'TEL-T3': float(await self.get('AUXILIARY.SENSOR[2].VALUE')),
            'TEL-T4': float(await self.get('AUXILIARY.SENSOR[4].VALUE')),
            'TEL-FOCU': float(await self.get('POSITION.INSTRUMENTAL.FOCUS.REALPOS'))
        }

    async def init_filters(self) -> None:
        # get number of filters
        num = int(await self.get('TELESCOPE.CONFIG.PORT[2].FILTER'))
        log.info("Found %d filters.", num)

        # loop all filters
        self._filters = []
        for i in range(num):
            # set filter
            await self.set('POINTING.SETUP.FILTER.INDEX', i)

            # get filter name
            name = await self.get('POINTING.SETUP.FILTER.NAME')

            # strip quotes
            name = name.replace('"', '')

            # append to list
            log.info('Found filter %s.', name)
            self._filters.append(name)

    async def filters(self) -> List[str]:
        if not self._filters:
            await self.init_filters()
        return self._filters

    async def change_filter(self, filter_name: str, force_forward: bool = True,
                            abort_event: Optional[asyncio.Event] = None) -> bool:
        # get current filter id
        cur_id = int(float(await self.get('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS')))

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

    async def _change_filter_to_id(self, filter_id: int, abort_event: Optional[asyncio.Event]= None) -> bool:
        # set it
        await self.set('POINTING.SETUP.FILTER.INDEX', filter_id)
        await self.set('POINTING.TRACK', 3)

        # wait for it
        return await self._wait_for_value('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS',
                                          filter_id, abort_event=abort_event)

    async def filter_name(self, filter_id: Optional[int] = None) -> str:
        if filter_id is None:
            filter_id = int(await self.get('POSITION.INSTRUMENTAL.FILTER[2].CURRPOS'))
        return self._filters[filter_id]

    async def stop(self) -> None:
        # stop telescope
        # TODO: there is obviously some kind of ABORT command, look into it

        # deactivate tracking
        await self.set('POINTING.TRACK', 0)
