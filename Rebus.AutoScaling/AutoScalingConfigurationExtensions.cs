using System;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.AutoScaling
{
    /// <summary>
    /// Configuration extensions for the auto-scaling extension
    /// </summary>
    public static class AutoScalingConfigurationExtensions
    {
        /// <summary>
        /// Enables auto-scaling. When enabled, the bus will always start out with one single worker, possibly adding workers
        /// up until <paramref name="maxNumberOfWorkers"/>.
        /// Max parallelism can be set with <paramref name="maxParallelism"/>, which would otherwise default to the same as the
        /// number of workers.
        /// At most one worker will be added/removed per second
        /// </summary>
        public static void EnableAutoScaling(this OptionsConfigurer configurer, int maxNumberOfWorkers, int? maxParallelism = null)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));

            // decorate the transport so the auto-scaler gets to see how many messages are received
            configurer.Decorate<ITransport>(c => c.Get<AutoScaler>());

            // register auto-scaler
            configurer.Register(c =>
            {
                var options = c.Get<Options>();

                options.MaxParallelism = maxParallelism ?? maxNumberOfWorkers;
                options.NumberOfWorkers = 1;

                var transport = c.Get<ITransport>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();

                return new AutoScaler(transport, rebusLoggerFactory, maxNumberOfWorkers, asyncTaskFactory, c.Get<IBus>);
            });

        }
    }
}
