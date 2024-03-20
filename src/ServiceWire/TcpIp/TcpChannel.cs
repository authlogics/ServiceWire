using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace ServiceWire.TcpIp
{
    public class TcpChannel : StreamingChannel
    {
        private Socket _client;
        private string _username;
        private string _password;
        private TcpChannelIdentifier _channelIdentifier;
        private bool _connected;

        /// <summary>
        /// Creates a connection to the concrete object handling method calls on the server side
        /// </summary>
        /// <param name="serviceType"></param>
        /// <param name="endpoint"></param>
        /// <param name="serializer">Inject your own serializer for complex objects and avoid using the Newtonsoft JSON DefaultSerializer.</param>
        /// <param name="compressor">Inject your own compressor and avoid using the standard GZIP DefaultCompressor.</param>
        public TcpChannel(Type serviceType, IPEndPoint endpoint, ISerializer serializer, ICompressor compressor)
            : base(serializer, compressor)
        {
            Initialize(null, null, serviceType, endpoint, 2500);
        }

        /// <summary>
        /// Creates a connection to the concrete object handling method calls on the server side
        /// </summary>
        /// <param name="serviceType"></param>
        /// <param name="endpoint"></param>
        /// <param name="serializer">Inject your own serializer for complex objects and avoid using the Newtonsoft JSON DefaultSerializer.</param>
        /// <param name="compressor">Inject your own compressor and avoid using the standard GZIP DefaultCompressor.</param>
        public TcpChannel(Type serviceType, TcpEndPoint endpoint, ISerializer serializer, ICompressor compressor)
            : base(serializer, compressor)
        {
            Initialize(null, null, serviceType, endpoint.EndPoint, endpoint.ConnectTimeOutMs);
        }

        /// <summary>
        /// Creates a secure connection to the concrete object handling method calls on the server side
        /// </summary>
        /// <param name="serviceType"></param>
        /// <param name="endpoint"></param>
        /// <param name="serializer">Inject your own serializer for complex objects and avoid using the Newtonsoft JSON DefaultSerializer.</param>
        /// <param name="compressor">Inject your own compressor and avoid using the standard GZIP DefaultCompressor.</param>
        public TcpChannel(Type serviceType, TcpZkEndPoint endpoint, ISerializer serializer, ICompressor compressor)
            : base(serializer, compressor)
        {
            if (endpoint == null) throw new ArgumentNullException("endpoint");
            if (endpoint.Username == null) throw new ArgumentNullException("endpoint.Username");
            if (endpoint.Password == null) throw new ArgumentNullException("endpoint.Password");
            Initialize(endpoint.Username, endpoint.Password,
                serviceType, endpoint.EndPoint, endpoint.ConnectTimeOutMs);
        }

        private void Initialize(string username, string password,
            Type serviceType, IPEndPoint endpoint, int connectTimeoutMs)
        {
            _username = username;
            _password = password;
            _serviceType = serviceType;
            _channelIdentifier = new TcpChannelIdentifier(endpoint);
            _client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp); // TcpClient(AddressFamily.InterNetwork);
            _client.LingerState.Enabled = false;

            var connectEventArgs = new SocketAsyncEventArgs
            {
                RemoteEndPoint = endpoint
            };

            try
            {
                _connected = false;

                connectEventArgs.Completed += CompletedEvent;

                if (_client.ConnectAsync(connectEventArgs))
                {
                    //operation pending - (false means completed synchronously)
                    while (!_connected)
                    {
                        if (!SpinWait.SpinUntil(() => _connected, connectTimeoutMs))
                        {
                            _client.Dispose();
                            throw new TimeoutException("Unable to connect within " + connectTimeoutMs + "ms");
                        }
                    }
                }

                if (connectEventArgs.SocketError != SocketError.Success)
                {
                    _client.Dispose();
                    throw new SocketException((int) connectEventArgs.SocketError);
                }

                if (!_client.Connected)
                {
                    _client.Dispose();
                    throw new SocketException((int) SocketError.NotConnected);
                }

                _stream = new BufferedStream(new NetworkStream(_client), 8192);
                _binReader = new BinaryReader(_stream);
                _binWriter = new BinaryWriter(_stream);
                try
                {
                    SyncInterface(_serviceType, _username, _password);
                }
                catch
                {
                    this.Dispose(true);
                    throw;
                }
            }
            finally
            {
                // All of this was found due to an application that grew to a run-time foot-print
                // of 5 Gb!  Running Redgate ANTS memory profiler found a set of objects that were
                // being retained in Gen2 due to a number of references related to the SocketAsyncEventArgs
                // each step in this was able to finally get a flat memory profile when making and breaking
                // connections between client and server.

                // Nulling these object references helps the garbage collector by breaking the 
                // references to the socket and endpoint objects.

                connectEventArgs.AcceptSocket = null;
                connectEventArgs.RemoteEndPoint = null;

                // Must explicitly remove the CompletedEvent from the Action handler
                // otherwise there is a circular reference which causes a number of objects
                // to not be garbage collected for each connection made and broken.

                connectEventArgs.Completed -= CompletedEvent;

                // If this isn't explicitly Disposed, there is a retained reference from
                // System.Threading.IOCompletionCallback

                connectEventArgs.Dispose();
            }
        }

        private void CompletedEvent(object sender, SocketAsyncEventArgs e)
        {
            _connected = true;
        }

        protected override IChannelIdentifier ChannelIdentifier => _channelIdentifier;

        public override bool IsConnected { get { return (null != _client) && _client.Connected; } }

        #region IDisposable override

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                _client.Dispose();
            }
        }

        #endregion
    }
}
