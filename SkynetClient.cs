// SkynetClient.cs
// Unity MonoBehaviour wrapper for SkynetTransport
// Handles reconnection and main-thread marshalling
// Does NOT auto-connect - user must call Connect() manually

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace SkynetUnity
{
    /// <summary>
    /// Unity client for Skynet protocol.
    /// Marshals packets to main thread and handles reconnection.
    /// Call Connect() manually to start connection.
    /// </summary>
    public class SkynetClient : MonoBehaviour
    {
        [Header("Network Config")]
        [SerializeField] private string _ip = "127.0.0.1";
        [SerializeField] private int _port = 10000;

        [Header("Reconnection")]
        [Tooltip("Enable automatic reconnection on disconnect")]
        [SerializeField] private bool _autoReconnect = true;

        [Tooltip("Initial retry delay in milliseconds")]
        [SerializeField] private int _initialRetryDelayMs = 1000;

        [Tooltip("Maximum retry delay in milliseconds")]
        [SerializeField] private int _maxRetryDelayMs = 10000;

        private SkynetTransport _transport;
        private CancellationTokenSource _connectionCts;
        private readonly ConcurrentQueue<Action> _mainThreadQueue = new ConcurrentQueue<Action>();

        private bool _isManuallyConnecting;
        private int _reconnectAttempts;

        /// <summary>
        /// Current connection status (best-effort).
        /// </summary>
        public bool IsConnected => _transport != null && _transport.IsConnected;

        /// <summary>
        /// Event fired on main thread when connected.
        /// </summary>
        public event Action OnConnectedEvent;

        /// <summary>
        /// Event fired on main thread when disconnected.
        /// </summary>
        public event Action OnDisconnectedEvent;

        /// <summary>
        /// Event fired on main thread for each received packet.
        /// Subscribe to this for game logic.
        /// </summary>
        public event Action<ushort, string> OnPacketEvent;

        private void Awake()
        {
            _transport = new SkynetTransport();

            // Bind events (invoked on background threads)
            _transport.OnPacketReceived += HandlePacketBackground;
            _transport.OnError += (msg) => Enqueue(() => Debug.LogError($"[Skynet] {msg}"));
            _transport.OnDisconnected += () => Enqueue(OnDisconnected);
        }

        /// <summary>
        /// Manually connect to server.
        /// Safe to call multiple times.
        /// </summary>
        public void Connect()
        {
            Connect(_ip, _port);
        }

        /// <summary>
        /// Manually connect to server with custom IP/port.
        /// Safe to call multiple times.
        /// </summary>
        public void Connect(string ip, int port)
        {
            if (_isManuallyConnecting) return;

            _ip = ip;
            _port = port;
            _isManuallyConnecting = true;

            // Cancel any existing connection attempt
            _connectionCts?.Cancel();
            _connectionCts?.Dispose();
            _connectionCts = new CancellationTokenSource();

            // Start connection loop
            ConnectLoop(_connectionCts.Token).SafeFireAndForget();
        }

        /// <summary>
        /// Connection/reconnection loop.
        /// Runs until Disconnect() is called or GameObject is destroyed.
        /// </summary>
        private async Task ConnectLoop(CancellationToken token)
        {
            int retryDelay = _initialRetryDelayMs;

            while (!token.IsCancellationRequested && _isManuallyConnecting)
            {
                try
                {
                    if (!_transport.IsConnected)
                    {
                        Debug.Log($"[Skynet] Connecting to {_ip}:{_port} (attempt {_reconnectAttempts + 1})...");

                        await _transport.ConnectAsync(_ip, _port, 5000);

                        retryDelay = _initialRetryDelayMs;
                        _reconnectAttempts = 0;

                        Enqueue(OnConnected);
                    }

                    await Task.Delay(1000, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    _reconnectAttempts++;
                    Debug.LogWarning($"[Skynet] Connection failed: {e.Message}. Retrying in {retryDelay}ms...");

                    // If auto-reconnect is disabled and first attempt failed, stop
                    if (!_autoReconnect && _reconnectAttempts > 0)
                    {
                        Debug.LogError("[Skynet] Auto-reconnect disabled. Stopping.");
                        _isManuallyConnecting = false;
                        break;
                    }

                    try
                    {
                        await Task.Delay(retryDelay, token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    // Exponential backoff with jitter to prevent thundering herd
                    retryDelay = Math.Min(retryDelay * 2, _maxRetryDelayMs);
                    retryDelay += UnityEngine.Random.Range(-100, 100);
                }
            }

            Debug.Log("[Skynet] Connection loop terminated");
        }

        private void OnConnected()
        {
            Debug.Log("[Skynet] Connected successfully!");
            _reconnectAttempts = 0;
            OnConnectedEvent?.Invoke();
        }

        private void OnDisconnected()
        {
            Debug.LogWarning("[Skynet] Disconnected");
            OnDisconnectedEvent?.Invoke();

            // If auto-reconnect is disabled, stop trying
            if (!_autoReconnect)
            {
                _isManuallyConnecting = false;
            }
        }

        /// <summary>
        /// Packet handler invoked on background thread.
        /// Marshals all packets to main thread.
        /// </summary>
        private void HandlePacketBackground(ushort cmdId, string body)
        {
            ushort op = (ushort)cmdId;
            Enqueue(() => OnPacketEvent?.Invoke(op, body));
        }

        /// <summary>
        /// Send packet to server. Thread-safe. Fire-and-forget safe.
        /// </summary>
        public void Send(ushort op, string body)
        {
            _transport.SendAsync(op, body).SafeFireAndForget(ex =>
            {
                Debug.LogError($"[Skynet] Send failed: {ex.Message}");
            });
        }

        /// <summary>
        /// Gracefully disconnect (stops auto-reconnect).
        /// </summary>
        public void Disconnect()
        {
            _isManuallyConnecting = false;
            _connectionCts?.Cancel();
            _transport?.Dispose();

            Debug.Log("[Skynet] Manually disconnected");
        }

        private void Enqueue(Action action) => _mainThreadQueue.Enqueue(action);

        private void Update()
        {
            // Adaptive batch processing prevents frame stalls
            int maxPerFrame = _mainThreadQueue.Count > 100 ? 100 : 50;
            int processed = 0;

            while (processed < maxPerFrame && _mainThreadQueue.TryDequeue(out var action))
            {
                try
                {
                    action?.Invoke();
                }
                catch (Exception e)
                {
                    Debug.LogError($"[Skynet] Main thread handler error: {e}");
                }
                processed++;
            }
        }

        private void OnDestroy()
        {
            _isManuallyConnecting = false;
            _connectionCts?.Cancel();

            _transport?.Dispose();
            _connectionCts?.Dispose();
        }

        private void OnApplicationQuit()
        {
            OnDestroy();
        }
    }
}