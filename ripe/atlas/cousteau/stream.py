# Copyright (c) 2016 RIPE NCC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import logging
import hashlib
from functools import partial

from socketio import Client, AsyncClient, ClientNamespace, AsyncClientNamespace

from .version import __version__


try:
    from logging import NullHandler
except ImportError:  # Python 2.6
    from logging import Handler

    class NullHandler(Handler):

        def emit(self, record):
            pass

LOG = logging.getLogger('atlas-stream')
LOG.addHandler(NullHandler())

class BaseNamespace():
    SUBSCRIPTIONS = {}

    def on_connect(self, *args):
        LOG.debug("Connected to RIPE Atlas Stream")

    def on_disconnect(self, *args):
        LOG.debug("Disconnected from RIPE Atlas Stream")

    def on_reconnect(self, *args):
        LOG.debug("Reconnected to RIPE Atlas Stream")
        LOG.debug("Trying to attach to existed subscriptions")
        self.resubscribe()

    def on_atlas_result(self, *args):
        LOG.info(args[0])

    def on_atlas_subscribed(self, *args):
        LOG.debug("Subscribed to subscription: {}".format(args[0]))
        hash_object = hashlib.sha1(str(args[0]).encode('utf-8'))
        hex_dig = hash_object.hexdigest()
        self.SUBSCRIPTIONS[hex_dig] = args[0]

    def on_atlas_error(self, *args):
        LOG.error("Got an error from stream server: {}".format(args[0]))

    def resubscribe(self):
        for subscription in self.SUBSCRIPTIONS.values():
            LOG.debug("Subscribing to {}".format(subscription))
            self.emit(AtlasStream.EVENT_NAME_SUBSCRIBE, subscription)

    def emit(self, *args):
        raise NotImplementedError('This class (BaseNamespace) cannot be used directly!')

class AtlasNamespace(BaseNamespace, ClientNamespace): pass

class AsyncAtlasNamespace(BaseNamespace, AsyncClientNamespace):
    async def resubscribe(self):
        for subscription in self.SUBSCRIPTIONS.values():
            LOG.debug("Subscribing to {}".format(subscription))
            await self.emit(AtlasStream.EVENT_NAME_SUBSCRIBE, subscription)



class AtlasStream(object):

    EVENT_NAME_RESULTS = "atlas_result"
    EVENT_NAME_SUBSCRIBE = "atlas_subscribe"
    EVENT_NAME_ERROR = "atlas_error"

    # Remove the following list when deprecation time expires
    CHANNELS = {
        "result": "atlas_result",
        "probe": "atlas_probestatus",
        "error": "atlas_error",
    }
    # -------------------------------------------------------

    def __init__(self, debug=False, server=False, session=None, headers=None):
        """Initialize stream"""
        self.iosocket_server = "https://atlas-stream.ripe.net:443"
        self.iosocket_resource = "/stream/socket.io"
        self.socketIO = AsyncClient(logger = LOG)
        self.debug = debug
        self.error_callback = None
        self.headers = headers or {}
        self.session = session
        self.namespace = AtlasNamespace()

        if not self.headers or not self.headers.get("User-Agent", None):
            user_agent = "RIPE ATLAS Cousteau v{0}".format(__version__)
            self.headers["User-Agent"] = user_agent

        if self.debug and server:
            self.iosocket_server = server

    def handle_error(self, error):
        if self.error_callback is not None:
            self.error_callback(error)
        else:
            print(error)

    def connect(self):
        """Initiate the channel we want to start streams from."""
        options = {
            "socketio_path": self.iosocket_resource,
            "headers": self.headers,
            "transports": ["websocket"],
        }

        if self.session is not None:
            options["http_session"] = self.session

        self.socketIO.on(self.EVENT_NAME_ERROR, self.handle_error)
        self.socketIO.register_namespace(self.namespace)

        return self.socketIO.connect(self.iosocket_server, **options)

    def disconnect(self):
        """Exits the channel k shuts down connection."""
        return self.socketIO.disconnect()

    def unpack_results(self, callback, data):
        if isinstance(data, list):
            for result in data:
                callback(result)
        else:
            callback(data)

    def bind_channel(self, channel, callback):
        """Bind given channel with the given callback"""

        # Remove the following list when deprecation time expires
        if channel in self.CHANNELS:
            warning = (
                "The event name '{}' will soon be deprecated. Use "
                "the real event name '{}' instead."
            ).format(channel, self.CHANNELS[channel])

            self.handle_error(warning)
            channel = self.CHANNELS[channel]
        # -------------------------------------------------------

        if channel == self.EVENT_NAME_ERROR:
            self.error_callback = callback
        elif channel == self.EVENT_NAME_RESULTS:
            self.socketIO.on(channel, partial(self.unpack_results, callback))
        else:
            self.socketIO.on(channel, callback)

    def start_stream(self, stream_type, **stream_parameters):
        """Starts new stream for given type with given parameters"""
        if stream_type:
            return self.subscribe(stream_type, **stream_parameters)
        else:
            self.handle_error("You need to set a stream type")

    def subscribe(self, stream_type, **parameters):
        """Subscribe to stream with give parameters."""
        parameters["stream_type"] = stream_type

        if (stream_type == "result") and ("buffering" not in parameters):
            parameters["buffering"] = True

        return self.socketIO.emit(self.EVENT_NAME_SUBSCRIBE, parameters)

    def timeout(self, seconds=None):
        """
        Times out all streams after n seconds or wait forever if seconds is
        None
        """
        if seconds is None:
            return self.socketIO.wait()
        else:
            raise NotImplementedError("Timeout handling is not implemented for 'wait'!")

class AsyncAtlasStream(AtlasStream):
    def __init__(self, debug=False, server=False, session=None, headers=None):
        super().__init__(debug=False, server=False, session=None, headers=None)

        self.socketIO = AsyncClient()
        self.namespace = AsyncAtlasNamespace()

    async def connect(self):
        await super().connect()

    async def disconnect(self):
        """Exits the channel k shuts down connection."""
        await super().disconnect()

    async def start_stream(self, stream_type, **stream_parameters):
        await super().start_stream(stream_type, **stream_parameters)

    async def subscribe(self, stream_type, **parameters):
        await super().subscribe(stream_type, **parameters)

    async def timeout(self, seconds=None):
        await super().timeout(seconds)
