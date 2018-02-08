using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;
// ReSharper disable RedundantArgumentDefaultValue
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.AutoScaling.Tests
{
    [TestFixture]
    public class StartupTest : FixtureBase
    {
        BuiltinHandlerActivator _activator;
        WorkerCounter _workerCounter;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "scaling-test"))
                .Options(o =>
                {
                    o.EnableAutoScaling(maxNumberOfWorkers: 10, adjustmentIntervalSeconds: 1);
                })
                .Start();

            _workerCounter = new WorkerCounter(_activator.Bus);

            Using(_workerCounter);
        }

        [Test]
        public async Task StartsOutWithOneSingleWorker()
        {
            _workerCounter.ReadingAdded += Console.WriteLine;

            await Task.Delay(5000);

            var averageNumberOfWorkers = _workerCounter.Readings
                .Select(r => r.WorkersCount)
                .Average();

            Assert.That(averageNumberOfWorkers, Is.EqualTo(1));
        }
    }
}
