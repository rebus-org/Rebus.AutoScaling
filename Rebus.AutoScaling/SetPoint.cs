using System;

namespace Rebus.AutoScaling
{
    class SetPoint
    {
        readonly decimal _minDiff;
        readonly decimal _fraction;

        public SetPoint(decimal minDiff, decimal fraction)
        {
            if (_minDiff < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minDiff), minDiff, "Min diff must be 0 or more");
            }

            if (!(fraction > 0 && fraction < 1))
            {
                throw new ArgumentOutOfRangeException(nameof(fraction), fraction, "Fraction must be between 0 and 1");
            }

            _minDiff = minDiff;
            _fraction = fraction;
        }

        public decimal Target { get; private set; }

        public decimal Value { get; private set; }

        public void Tick()
        {
            var diff = Target - Value;

            if (diff < _minDiff)
            {
                Value = Target;
                return;
            }

            var delta = diff*_fraction;

            Value += delta;
        }
    }
}