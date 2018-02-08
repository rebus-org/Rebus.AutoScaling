using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Transport;

#pragma warning disable 1998

namespace Rebus.AutoScaling
{
    class AutoScaler : IDisposable, IInitializable, ITransport
    {
        readonly ITransport _transport;
        readonly int _maximumNumberOfWorkers;
        readonly Func<IBus> _busFactory;
        readonly ILog _logger;
        readonly IAsyncTask _task;

        bool _disposed;

        public AutoScaler(ITransport transport, IRebusLoggerFactory rebusLoggerFactory, int maximumNumberOfWorkers, IAsyncTaskFactory asyncTaskFactory, Func<IBus> busFactory, int adjustmentIntervalSeconds)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));
            
            _logger = rebusLoggerFactory.GetLogger<AutoScaler>();
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _maximumNumberOfWorkers = maximumNumberOfWorkers;
            _busFactory = busFactory ?? throw new ArgumentNullException(nameof(busFactory));
            _task = asyncTaskFactory.Create("AutoScale", Tick, intervalSeconds: adjustmentIntervalSeconds);
        }

        public void Initialize()
        {
            _logger.Info("Initializing auto-scaler - will add up to {0} workers", _maximumNumberOfWorkers);
            _task.Start();
        }

        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _logger.Info("Stopping auto-scaler");
                _task.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }

        public void CreateQueue(string address)
        {
            _transport.CreateQueue(address);
        }

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            return _transport.Send(destinationAddress, message, context);
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            var transportMessage = await _transport.Receive(context, cancellationToken);
            var ticks = DateTime.UtcNow.Ticks;

            Interlocked.Exchange(ref _lastReceiveCallTime, ticks);

            if (transportMessage == null)
            {
                Interlocked.Exchange(ref _lastNullReceiveTime, ticks);
                Interlocked.Exchange(ref _successiveReceivesWithMessage, 0);
            }
            else
            {
                Interlocked.Exchange(ref _lastNullReceiveTime, 0);
                Interlocked.Increment(ref _successiveReceivesWithMessage);
            }

            return transportMessage;
        }

        public string Address => _transport.Address;

        long _lastReceiveCallTime;
        long _lastNullReceiveTime;
        long _successiveReceivesWithMessage;

        IBus _bus;

        async Task Tick()
        {
            var bus = GetBus();
            var currentNumberOfWorkers = bus.Advanced.Workers.Count;

            // bus has not been started yet or some other code has set it to zero
            if (currentNumberOfWorkers == 0) return;

            var scaleAction = DecideScaleAction();

            switch (scaleAction)
            {
                case ScaleAction.AddWorker:
                    if (currentNumberOfWorkers >= _maximumNumberOfWorkers) return;
                    SetNumberOfWorkers(currentNumberOfWorkers + 1);
                    break;
                case ScaleAction.RemoveWorker:
                    if (currentNumberOfWorkers == 1) return;
                    SetNumberOfWorkers(currentNumberOfWorkers - 1);
                    break;
            }
        }

        void SetNumberOfWorkers(int newNumberOfWorkers)
        {
            _logger.Debug("Auto-scale to {0} workers", newNumberOfWorkers);
            GetBus().Advanced.Workers.SetNumberOfWorkers(newNumberOfWorkers);
        }

        enum ScaleAction
        {
            AddWorker,
            RemoveWorker,

            NoChange
        }

        IBus GetBus()
        {
            return _bus ?? (_bus = _busFactory());
        }

        ScaleAction DecideScaleAction()
        {
            var lastNullTicks = Interlocked.Read(ref _lastNullReceiveTime);
            var lastNullTime = new DateTime(lastNullTicks);
            var elapsedSinceLastNull = DateTime.UtcNow - lastNullTime;
            var itHasNotBeenLongSinceWeReceivedNull = elapsedSinceLastNull < TimeSpan.FromSeconds(1);

            if (itHasNotBeenLongSinceWeReceivedNull) return ScaleAction.RemoveWorker;

            var lastReadTicks = Interlocked.Read(ref _lastReceiveCallTime);
            var lastReadTime = new DateTime(lastReadTicks);
            var elapsedSinceLastRead = DateTime.UtcNow - lastReadTime;
            var itHasBeenLongSinceLastAttemptedReceive = elapsedSinceLastRead > TimeSpan.FromSeconds(1);

            if (itHasBeenLongSinceLastAttemptedReceive) return ScaleAction.AddWorker;

            var successiveReceivesWithMessage = Interlocked.Read(ref _successiveReceivesWithMessage);
            var receivedManyMessagesInSuccesion = successiveReceivesWithMessage > 100;

            if (receivedManyMessagesInSuccesion) return ScaleAction.AddWorker;

            return ScaleAction.NoChange;
        }
    }
}