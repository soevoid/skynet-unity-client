// SkynetTransport.cs
// Pure C# TCP transport layer for Skynet protocol
// Thread-safe, allocation-optimized, Unity-agnostic

using System;
using System.Buffers;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SkynetUnity
{
    /// <summary>
    /// Low-level TCP transport implementing Skynet binary protocol.
    /// Thread-safe. Uses ArrayPool for zero-allocation sends.
    /// Pure transport layer - no game logic.
    /// </summary>
    public class SkynetTransport : IDisposable
    {
        private TcpClient _client;
        private NetworkStream _stream;
        private CancellationTokenSource _readCts;

        // Thread Safety
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);
        private int _isDisposed = 0;

        // Buffers: Separate to prevent header/payload corruption
        private readonly byte[] _headerBuffer = new byte[2];
        private readonly byte[] _readBuffer = new byte[65535];

        /// <summary>
        /// Best-effort connection status check.
        /// Note: TCP state is unreliable; use application-level heartbeat for true liveness.
        /// </summary>
        public bool IsConnected =>
            _isDisposed == 0 &&
            _client != null &&
            _client.Connected &&
            _stream != null &&
            _stream.CanRead;

        // Events (invoked on background threads - marshal to main thread if needed)
        public event Action<ushort, string> OnPacketReceived;
        public event Action<string> OnError;
        public event Action OnDisconnected;

        /// <summary>
        /// Connect to Skynet server with timeout.
        /// Safe to call multiple times (idempotent).
        /// </summary>
        public async Task ConnectAsync(string ip, int port, int timeoutMs = 5000)
        {
            if (IsConnected) return;

            // Clean up any previous connection
            Dispose();
            _isDisposed = 0;

            try
            {
                _client = new TcpClient
                {
                    NoDelay = true,  // Disable Nagle for real-time gaming
                    ReceiveBufferSize = 65535,
                    SendBufferSize = 65535
                };

                // Task.WhenAny for universal platform compatibility
                var connectTask = _client.ConnectAsync(ip, port);
                var timeoutTask = Task.Delay(timeoutMs);

                var completed = await Task.WhenAny(connectTask, timeoutTask);
                if (completed == timeoutTask)
                {
                    throw new TimeoutException($"Connection to {ip}:{port} timed out after {timeoutMs}ms");
                }
                await connectTask; // Rethrow any connection exceptions

                if (!_client.Connected)
                    throw new SocketException((int)SocketError.ConnectionRefused);

                _stream = _client.GetStream();
                _readCts = new CancellationTokenSource();

                // Start read loop on background thread
                _ = Task.Run(() => ReadLoopAsync(_readCts.Token), _readCts.Token);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Send packet to server. Thread-safe. Fire-and-forget safe.
        /// Uses ArrayPool for zero-allocation sends.
        /// </summary>
        public async Task SendAsync(ushort op, string message)
        {
            if (!IsConnected) return;

            // Calculate exact size needed
            int bodyByteCount = Encoding.UTF8.GetByteCount(message);
            int totalLen = 4 + bodyByteCount; // Header(2) + Cmd(2) + Body

            byte[] buffer = ArrayPool<byte>.Shared.Rent(totalLen);

            try
            {
                // Encode header: payload length = cmd(2) + body
                ushort payloadLen = (ushort)(2 + bodyByteCount);
                buffer[0] = (byte)(payloadLen >> 8);
                buffer[1] = (byte)(payloadLen & 0xFF);

                // Encode command
                ushort cmd = (ushort)op;
                buffer[2] = (byte)(cmd >> 8);
                buffer[3] = (byte)(cmd & 0xFF);

                // Encode body directly into buffer
                Encoding.UTF8.GetBytes(message, 0, message.Length, buffer, 4);

                await _writeLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (!IsConnected) return;

                    // TCP auto-flushes on write
                    await _stream.WriteAsync(buffer, 0, totalLen).ConfigureAwait(false);
                }
                finally
                {
                    _writeLock.Release();
                }
            }
            catch (Exception e)
            {
                if (IsConnected)
                {
                    OnError?.Invoke($"Send Error: {e.GetType().Name} - {e.Message}");
                    Dispose();
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private async Task ReadLoopAsync(CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested && IsConnected)
                {
                    // Read header into dedicated buffer (prevents corruption)
                    if (!await ReadExactlyAsync(_headerBuffer, 0, 2, token)) break;

                    ushort payloadLen = (ushort)((_headerBuffer[0] << 8) | _headerBuffer[1]);

                    // Validation
                    if (payloadLen < 2)
                    {
                        OnError?.Invoke($"Protocol Error: Invalid payload length {payloadLen}");
                        break;
                    }
                    if (payloadLen > _readBuffer.Length)
                    {
                        OnError?.Invoke($"Protocol Error: Packet too large ({payloadLen} bytes)");
                        break;
                    }

                    // Read payload
                    if (!await ReadExactlyAsync(_readBuffer, 0, payloadLen, token)) break;

                    // Parse command
                    ushort cmdId = (ushort)((_readBuffer[0] << 8) | _readBuffer[1]);

                    // Only allocate string if there's body content
                    string body = payloadLen > 2
                        ? Encoding.UTF8.GetString(_readBuffer, 2, payloadLen - 2)
                        : string.Empty;

                    // Invoke handlers - don't let exceptions kill read loop
                    try
                    {
                        OnPacketReceived?.Invoke(cmdId, body);
                    }
                    catch (Exception e)
                    {
                        OnError?.Invoke($"Packet handler error: {e.Message}");
                    }
                }
            }
            catch (OperationCanceledException) { /* Clean exit */ }
            catch (Exception e)
            {
                if (IsConnected)
                {
                    OnError?.Invoke($"Read Loop Error: {e.GetType().Name} - {e.Message}");
                }
            }
            finally
            {
                Dispose();
            }
        }

        private async Task<bool> ReadExactlyAsync(byte[] buffer, int offset, int count, CancellationToken token)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                if (!IsConnected) return false;

                int bytesRead = await _stream.ReadAsync(buffer, offset + totalRead, count - totalRead, token)
                    .ConfigureAwait(false);

                if (bytesRead == 0) return false; // Connection closed

                totalRead += bytesRead;
            }
            return true;
        }

        /// <summary>
        /// Dispose connection. Idempotent and thread-safe.
        /// Safe to call multiple times.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 1) return;

            try
            {
                // Cancel first to signal all async operations
                _readCts?.Cancel();

                // Dispose in correct order: Stream → Client → CancellationTokenSource
                try { _stream?.Dispose(); } catch { }
                try { _client?.Dispose(); } catch { }
                _readCts?.Dispose();

                // Do NOT dispose _writeLock - let GC handle it to avoid races
            }
            catch { /* Suppress cleanup errors */ }

            OnDisconnected?.Invoke();
        }
    }
}