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

        public AutoScaler(ITransport transport, IRebusLoggerFactory rebusLoggerFactory, int maximumNumberOfWorkers, IAsyncTaskFactory asyncTaskFactory, Func<IBus> busFactory)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));
            if (busFactory == null) throw new ArgumentNullException(nameof(busFactory));
            _logger = rebusLoggerFactory.GetCurrentClassLogger();
            _transport = transport;
            _maximumNumberOfWorkers = maximumNumberOfWorkers;
            _busFactory = busFactory;
            _task = asyncTaskFactory.Create("AutoScale", Tick, intervalSeconds: 1);
        }

        public void Initialize()
        {
            _logger.Info("Initializing auto-scaler - will add up to {0} workers", _maximumNumberOfWorkers);
            _task.Start();
        }

        public void Dispose()
        {
            _logger.Info("Stopping auto-scaler");
            _task.Dispose();
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
            }
            else
            {
                Interlocked.Exchange(ref _lastNullReceiveTime, 0);
            }

            return transportMessage;
        }

        public string Address => _transport.Address;

        long _lastReceiveCallTime;
        long _lastNullReceiveTime;

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

            return ScaleAction.NoChange;
        }
    }
}