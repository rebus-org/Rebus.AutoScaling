using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Timers;
using Rebus.Bus;

namespace Rebus.AutoScaling.Tests
{
    public class WorkerCounter : IDisposable
    {
        readonly IBus _bus;
        readonly Timer _timer = new Timer(1000);
        readonly ConcurrentQueue<Reading> _readings = new ConcurrentQueue<Reading>();

        public WorkerCounter(IBus bus)
        {
            if (bus == null) throw new ArgumentNullException(nameof(bus));

            _bus = bus;
            _timer.Elapsed += (o, ea) => AddReading();
            _timer.Start();
        }

        public IEnumerable<Reading> Readings => _readings.ToList();

        public event Action<Reading> ReadingAdded;

        void AddReading()
        {
            var workersCount = _bus.Advanced.Workers.Count;
            var time = DateTimeOffset.Now;
            var reading = new Reading(time, workersCount);

            _readings.Enqueue(reading);

            ReadingAdded?.Invoke(reading);
        }

        public void Dispose()
        {
            _timer.Dispose();
        }

        public class Reading
        {
            public Reading(DateTimeOffset time, decimal workersCount)
            {
                Time = time;
                WorkersCount = workersCount;
            }

            public DateTimeOffset Time { get; }
            public decimal WorkersCount { get; }

            public override string ToString()
            {
                var bar = new string('*', (int)WorkersCount);

                if (WorkersCount != (int) WorkersCount)
                {
                    bar = bar + ".";
                }

                return $"{Time:s}: {bar} ({WorkersCount:0.#})";
            }
        }
    }
}